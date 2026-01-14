use crate::arb_finder::{ArbFinder, ArbFinderConfig, ArbPrediction, MarketInfo as ArbMarketInfo};
use crate::clob_api::fetch_token_balances;
use crate::config::Config;
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderState, SideLadderConfig};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, now_ms};
use crate::poller::{
    LedgerHandle, MarketSide, spawn_ledger_actor, spawn_order_feed, spawn_reconciliation_poller,
};
use crate::price_feed::{PriceCache, spawn_price_feed};
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::{Timelike, Utc};
use futures::stream::{self, StreamExt};
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{Side as ClobSide, SignatureType, SignedOrder};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal as PolyDecimal};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, trace, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;
type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(10); // Fast loop for arb detection
const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";
const PREFETCH_CONCURRENCY: usize = 6;
const SIGN_CONCURRENCY: usize = 4;
const MIN_VALID_PRICE: Decimal = dec!(0.01);
const DYNAMIC_MIN_ORDER_SIZE: Decimal = dec!(5);
const DYNAMIC_MAX_ORDER_SIZE: Decimal = dec!(25);
const SIZE_STEP: Decimal = dec!(0.5);
const EXPENSIVE_BUFFER: Decimal = dec!(0.01);
const HEDGE_MAX_LOSS: Decimal = dec!(0.02);
const HEDGE_MIN_IMBALANCE_MULT: Decimal = dec!(2.0);
const HEDGE_TIME_WINDOW: Duration = Duration::from_secs(120);
const EXTREME_HIGH_PRICE: Decimal = dec!(0.9);
const EXTREME_LOW_PRICE: Decimal = dec!(0.1);
const EXTREME_MOMENTUM_THRESHOLD: Decimal = dec!(0.002);
const MOMENTUM_FAST_WINDOW: Duration = Duration::from_secs(3);
const MOMENTUM_SLOW_WINDOW: Duration = Duration::from_secs(12);
const VOLATILITY_WINDOW: Duration = Duration::from_secs(10);
const DEPTH_WINDOW: Decimal = dec!(0.02);
const DEPTH_SIZE_DIVISOR: Decimal = dec!(10);
const MOMENTUM_SCALE: Decimal = dec!(0.02);
const VOLATILITY_SCALE: Decimal = dec!(0.03);

/// Consolidated view of a market we're trading
#[derive(Clone)]
pub struct ActiveMarket {
    pub market_id: String,
    pub asset: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub end_time: chrono::DateTime<Utc>,
}

impl ActiveMarket {
    fn from_info(info: &MarketInfo) -> Self {
        Self {
            market_id: info.id.clone(),
            asset: info.asset.clone(),
            up_token_id: info.up_token_id.clone(),
            down_token_id: info.down_token_id.clone(),
            end_time: info.end_time,
        }
    }

    fn token_ids(&self) -> [&str; 2] {
        [&self.up_token_id, &self.down_token_id]
    }
}

pub struct SimpleBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    ws_client: Arc<AuthenticatedWsClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,

    /// Active markets keyed by market_id
    markets: HashMap<String, ActiveMarket>,

    /// Timing state
    last_market_refresh: Instant,
    last_order_by_market: HashMap<String, Instant>,

    /// Single source of truth for inventory
    ledger: LedgerHandle,

    /// Price data from WebSocket
    price_cache: Arc<RwLock<PriceCache>>,

    /// Background tasks
    _order_feed: Option<tokio::task::JoinHandle<()>>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    _reconciliation_poller: tokio::task::JoinHandle<()>,

    /// Ladder strategy
    ladder_engine: LadderEngine,
    ladder_state: LadderState,

    /// Predictive arb finder
    arb_finder: ArbFinder,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

        let addr = Address::from_str(config.polymarket_proxy_address.trim())
            .map_err(|e| anyhow::anyhow!("Invalid POLYMARKET_PROXY_ADDRESS: {}", e))?;

        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));

        let rest_client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?;
        let credentials: Credentials = rest_client
            .create_or_derive_api_key(&signer_with_chain, None)
            .await?;

        let client = Arc::new(
            rest_client
                .authentication_builder(&signer_with_chain)
                .funder(addr)
                .signature_type(SignatureType::Proxy)
                .authenticate()
                .await?,
        );

        let ws_client = Arc::new(
            WsClient::new(WS_SUB_URL, Default::default())?.authenticate(credentials, addr)?,
        );

        // Spawn ledger actor - single source of truth
        let ledger = spawn_ledger_actor();

        // HTTP reconciliation as backup (every 10s)
        let reconciliation_poller =
            spawn_reconciliation_poller(client.clone(), ledger.clone(), Duration::from_secs(10));

        let target_assets = config.target_assets.clone();

        // Initialize arb finder with its own config
        let arb_finder_config = ArbFinderConfig::from_env();
        let arb_finder = ArbFinder::new(arb_finder_config);

        Ok(Self {
            config,
            client,
            ws_client,
            signer,
            market_cache: MarketCache::new(target_assets),
            markets: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            ledger,
            price_cache: Arc::new(RwLock::new(PriceCache::default())),
            _order_feed: None,
            _price_feed: None,
            _reconciliation_poller: reconciliation_poller,
            ladder_engine: LadderEngine::new(LadderConfig::default()),
            ladder_state: LadderState::default(),
            arb_finder,
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;
        info!("Entering main loop...");

        loop {
            if self.should_refresh_markets() {
                self.discover_markets().await;
                self.last_market_refresh = Instant::now();
            }

            self.scan().await;
            tokio::time::sleep(LOOP_DELAY).await;
        }
    }

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");

        // Fetch and filter markets
        let markets = match self.fetch_active_markets().await {
            Some(m) => m,
            None => return,
        };

        // Update internal state
        self.markets = markets
            .iter()
            .map(|m| (m.market_id.clone(), m.clone()))
            .collect();

        // Log what we found
        for market in &markets {
            info!(
                market_id = %market.market_id,
                asset = %market.asset,
                end_time = %market.end_time,
                "Market registered"
            );
        }

        // Warm token metadata caches, then initialize positions and start feeds
        self.prefetch_token_metadata(&markets).await;
        self.init_positions(&markets).await;
        self.start_feeds(&markets);

        // Initialize arb finder with markets
        let arb_markets: Vec<ArbMarketInfo> = markets
            .iter()
            .map(|m| ArbMarketInfo {
                market_id: m.market_id.clone(),
                up_token_id: m.up_token_id.clone(),
                down_token_id: m.down_token_id.clone(),
                end_time: m.end_time,
            })
            .collect();
        self.arb_finder.init_markets(arb_markets);
        self.arb_finder.start_trade_poller(self.client.clone());
    }

    async fn fetch_active_markets(&self) -> Option<Vec<ActiveMarket>> {
        let all_markets = match self.market_cache.get_markets(Utc::now()).await {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "Failed to fetch markets");
                return None;
            }
        };

        let now = Utc::now();

        // Filter active, dedupe by asset (keep soonest-ending per asset)
        let mut by_asset: HashMap<String, ActiveMarket> = HashMap::new();
        for info in all_markets {
            if info.start_time <= now && info.end_time > now {
                let entry = by_asset
                    .entry(info.asset.clone())
                    .or_insert_with(|| ActiveMarket::from_info(&info));
                if info.end_time < entry.end_time {
                    *entry = ActiveMarket::from_info(&info);
                }
            }
        }

        Some(by_asset.into_values().collect())
    }

    async fn init_positions(&self, markets: &[ActiveMarket]) {
        for market in markets {
            match fetch_token_balances(&self.client, &market.up_token_id, &market.down_token_id)
                .await
            {
                Ok((up_bal, down_bal)) => {
                    self.ledger
                        .set_initial_position(market.market_id.clone(), up_bal, down_bal)
                        .await;

                    info!(
                        market_id = %market.market_id,
                        up_shares = %up_bal,
                        down_shares = %down_bal,
                        "Position initialized"
                    );
                }
                Err(e) => {
                    warn!(
                        market_id = %market.market_id,
                        error = %e,
                        "Failed to fetch initial position"
                    );
                }
            }
        }
    }

    async fn prefetch_token_metadata(&self, markets: &[ActiveMarket]) {
        let mut token_ids = Vec::new();
        for market in markets {
            token_ids.push(market.up_token_id.clone());
            token_ids.push(market.down_token_id.clone());
        }
        token_ids.sort();
        token_ids.dedup();

        let client = self.client.clone();
        stream::iter(token_ids)
            .map(|token_id| {
                let client = client.clone();
                async move {
                    let _ = client.tick_size(&token_id).await;
                    let _ = client.neg_risk(&token_id).await;
                }
            })
            .buffer_unordered(PREFETCH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
    }

    fn start_feeds(&mut self, markets: &[ActiveMarket]) {
        // Abort existing feeds
        if let Some(h) = self._price_feed.take() {
            h.abort();
        }
        if let Some(h) = self._order_feed.take() {
            h.abort();
        }

        // Collect IDs
        let market_ids: Vec<String> = markets.iter().map(|m| m.market_id.clone()).collect();
        let token_ids: Vec<String> = markets
            .iter()
            .flat_map(|m| [m.up_token_id.clone(), m.down_token_id.clone()])
            .collect();

        // Start price feed
        match spawn_price_feed(WS_SUB_URL, token_ids, self.price_cache.clone()) {
            Ok(handle) => {
                self._price_feed = Some(handle);
                info!("Price feed started");
            }
            Err(e) => {
                error!(error = %e, "Failed to start price feed");
            }
        }

        // Start order feed (WebSocket for fills)
        self._order_feed = Some(spawn_order_feed(
            self.ws_client.clone(),
            self.ledger.clone(),
            market_ids,
        ));
        info!("Order feed started");
    }

    async fn scan(&mut self) {
        // Snapshot markets to avoid borrow issues
        let markets: Vec<ActiveMarket> = self.markets.values().cloned().collect();

        // Update arb finder with latest prices from cache
        {
            let cache = self.price_cache.read();
            for market in &markets {
                if let Some((bid, ask)) = cache.get(&market.up_token_id) {
                    self.arb_finder
                        .update_ws_price(&market.up_token_id, bid, ask);
                }
                if let Some((bid, ask)) = cache.get(&market.down_token_id) {
                    self.arb_finder
                        .update_ws_price(&market.down_token_id, bid, ask);
                }
            }
        }

        // Check arb finder for predictions first (used to bias ladder behavior)
        let predictions = self.arb_finder.scan();
        let mut prediction_by_market: HashMap<String, ArbPrediction> = HashMap::new();
        for prediction in predictions {
            debug!(
                market_id = %prediction.market_id,
                confidence = %prediction.confidence,
                action = ?prediction.recommended_action,
                up_target = ?prediction.up_target_price,
                down_target = ?prediction.down_target_price,
                signals = prediction.signals.len(),
                "Arb prediction"
            );
            let entry = prediction_by_market
                .entry(prediction.market_id.clone())
                .or_insert_with(|| prediction.clone());
            if prediction.confidence > entry.confidence {
                *entry = prediction;
            }
        }

        // Normal evaluation for each market
        for market in markets {
            let prediction = prediction_by_market.get(&market.market_id);
            if let Some(action) = self.evaluate_market(&market, prediction).await {
                self.execute_action(&market, action).await;
            }
        }
    }

    /// Evaluate a single market, returning an action if needed
    async fn evaluate_market(
        &self,
        market: &ActiveMarket,
        prediction: Option<&ArbPrediction>,
    ) -> Option<MarketAction> {
        // Get prices (and enforce freshness)
        let (up_ask, down_ask) = {
            let now_ms_u64 = now_ms().max(0) as u64;
            let max_age_ms = self.config.max_price_age_ms.max(0) as u64;

            let cache = self.price_cache.read();

            let (_, up_ask, up_age) = cache.get_with_age(&market.up_token_id, now_ms_u64)?;
            let (_, down_ask, down_age) = cache.get_with_age(&market.down_token_id, now_ms_u64)?;

            if up_age > max_age_ms || down_age > max_age_ms {
                trace!(
                    market_id = %market.market_id,
                    up_age_ms = up_age,
                    down_age_ms = down_age,
                    max_age_ms,
                    "Skipping market - stale prices"
                );
                return None;
            }

            // Sanity-check price bounds
            if up_ask < MIN_VALID_PRICE
                || down_ask < MIN_VALID_PRICE
                || up_ask >= Decimal::ONE
                || down_ask >= Decimal::ONE
            {
                warn!(
                    market_id = %market.market_id,
                    up_ask = %up_ask,
                    down_ask = %down_ask,
                    "Skipping market - invalid ask price"
                );
                return None;
            }

            (up_ask, down_ask)
        };

        let combined = up_ask + down_ask;
        let spread_from_arb = combined - self.config.arb_threshold;

        if spread_from_arb < dec!(0.01) {
            debug!(
                up_ask = %up_ask,
                down_ask = %down_ask,
            );
        } else {
            trace!(
                market_id = %market.market_id,
                up_ask = %up_ask,
                down_ask = %down_ask,
                "Price check"
            );
        }

        // PRIORITY 1: Check for arbitrage opportunity
        if combined < self.config.arb_threshold {
            let profit_per_pair = Decimal::ONE - combined;
            info!(
                market_id = %market.market_id,
                up_ask = %up_ask,
                down_ask = %down_ask,
                combined = %combined,
                profit_per_pair = %profit_per_pair,
                "🎯 ARB OPPORTUNITY DETECTED"
            );

            // Calculate size - use order_size from config
            let arb_size = self.config.order_size;

            return Some(MarketAction::Arb {
                up_ask,
                down_ask,
                combined,
                up_size: arb_size,
                down_size: arb_size,
            });
        }

        // PRIORITY 2: Normal ladder maintenance (only if cooldown elapsed)
        let should_reladder = self.ladder_state.should_reladder(
            &market.market_id,
            up_ask,
            down_ask,
            self.ladder_engine.config().reladder_threshold,
        );

        if !should_reladder && !self.cooldown_elapsed(&market.market_id) {
            return None;
        }

        // Get ledger state
        let snapshot = self.ledger.get_state(&market.market_id).await;

        // Compute ladder with dynamic sizing if we have arb-finder state
        let plan = if let Some(state) = self.arb_finder.state_store().get_state(&market.market_id) {
            let cheap_is_up = up_ask <= down_ask;
            let (cheap_ask, expensive_ask) = if cheap_is_up {
                (up_ask, down_ask)
            } else {
                (down_ask, up_ask)
            };
            let expensive_cap =
                (self.config.target_total_cost - cheap_ask + EXPENSIVE_BUFFER).max(dec!(0.01));

            let imbalance = (snapshot.position.up_shares - snapshot.position.down_shares).abs();
            let time_to_end = market.end_time - Utc::now();
            let allow_expensive = self.should_allow_expensive(
                cheap_ask,
                expensive_ask,
                expensive_cap,
                imbalance,
                time_to_end,
                prediction.map(|p| p.confidence),
            );

            let up_metrics = self.compute_side_metrics(
                &state.up,
                snapshot.position.up_shares,
                snapshot.position.down_shares,
                cheap_is_up,
            );
            let down_metrics = self.compute_side_metrics(
                &state.down,
                snapshot.position.down_shares,
                snapshot.position.up_shares,
                !cheap_is_up,
            );

            let up_config = self.build_side_ladder_config(
                &up_metrics,
                &down_metrics,
                cheap_is_up,
                allow_expensive,
            );
            let down_config = self.build_side_ladder_config(
                &down_metrics,
                &up_metrics,
                !cheap_is_up,
                allow_expensive,
            );

            let mut plan = self.ladder_engine.compute_dynamic_ladder(
                up_ask,
                down_ask,
                &snapshot.position,
                snapshot.pending_up,
                snapshot.pending_down,
                &snapshot.open_orders,
                up_config,
                down_config,
            );

            self.filter_async_orders(
                &mut plan,
                &up_metrics,
                &down_metrics,
                &snapshot.position,
                cheap_is_up,
                up_ask,
                down_ask,
                allow_expensive,
            );
            plan
        } else {
            self.ladder_engine.compute_ladder(
                up_ask,
                down_ask,
                &snapshot.position,
                snapshot.pending_up,
                snapshot.pending_down,
                &snapshot.open_orders,
            )
        };

        if plan.cancellations.is_empty() && plan.orders.is_empty() {
            return None;
        }

        Some(MarketAction::Ladder {
            up_ask,
            down_ask,
            cancellations: plan.cancellations,
            orders: plan.orders,
        })
    }

    fn compute_side_metrics(
        &self,
        state: &crate::arb_finder::market_state::TokenState,
        side_shares: Decimal,
        other_shares: Decimal,
        is_cheap: bool,
    ) -> SideMetrics {
        let momentum = state
            .rest_ema_slope(MOMENTUM_FAST_WINDOW, MOMENTUM_SLOW_WINDOW)
            .unwrap_or(Decimal::ZERO);
        let volatility = state
            .rest_price_range(VOLATILITY_WINDOW)
            .unwrap_or(Decimal::ZERO);
        let depth = state
            .ask_depth_within(DEPTH_WINDOW)
            .unwrap_or(Decimal::ZERO);

        let base_size = clamp_decimal(
            depth / DEPTH_SIZE_DIVISOR,
            DYNAMIC_MIN_ORDER_SIZE,
            DYNAMIC_MAX_ORDER_SIZE,
        );

        let target = self
            .ladder_engine
            .config()
            .target_per_side
            .max(Decimal::ONE);
        let imbalance = (other_shares - side_shares) / target;
        let imbalance_factor = clamp_decimal(
            Decimal::ONE + (imbalance.max(Decimal::ZERO) * dec!(0.6)),
            dec!(0.7),
            dec!(1.6),
        );

        let momentum_score = (momentum.abs() / MOMENTUM_SCALE).min(Decimal::ONE);
        let momentum_factor = Decimal::ONE + (dec!(0.4) * momentum_score);
        let volatility_score = (volatility / VOLATILITY_SCALE).min(Decimal::ONE);
        let volatility_factor = Decimal::ONE + (dec!(0.3) * volatility_score);
        let price_bias = if is_cheap { dec!(1.2) } else { dec!(0.8) };

        let size = base_size * momentum_factor * volatility_factor * imbalance_factor * price_bias;
        let size = round_to_step(size, SIZE_STEP);

        SideMetrics {
            momentum,
            volatility,
            depth,
            size,
            priority: size,
        }
    }

    fn build_side_ladder_config(
        &self,
        side: &SideMetrics,
        other: &SideMetrics,
        is_cheap: bool,
        allow_expensive: bool,
    ) -> SideLadderConfig {
        let mut levels = if is_cheap { 3 } else { 2 };
        if !is_cheap && !allow_expensive {
            levels = 1;
        }

        if side.volatility > dec!(0.02) {
            levels = levels.max(3);
        }

        if side.size < DYNAMIC_MIN_ORDER_SIZE * Decimal::from(levels as u32) {
            levels = 1;
        }

        let mut size_per_level =
            (side.size / Decimal::from(levels as u32)).max(DYNAMIC_MIN_ORDER_SIZE);
        if !is_cheap && !allow_expensive {
            size_per_level = Decimal::ZERO;
        }

        let fast_side = side.momentum.abs() >= other.momentum.abs();
        let top_offset = if fast_side && is_cheap {
            dec!(0.01)
        } else {
            dec!(0.02)
        };

        let spacing = if side.volatility > dec!(0.02) {
            dec!(0.01)
        } else {
            dec!(0.01)
        };

        SideLadderConfig {
            levels,
            spacing,
            size_per_level,
            top_offset,
        }
    }

    fn filter_async_orders(
        &self,
        plan: &mut crate::ladder::LadderPlan,
        up: &SideMetrics,
        down: &SideMetrics,
        position: &crate::poller::MarketPosition,
        cheap_is_up: bool,
        up_ask: Decimal,
        down_ask: Decimal,
        allow_expensive: bool,
    ) {
        let up_orders: Vec<LadderOrder> = plan
            .orders
            .iter()
            .cloned()
            .filter(|o| o.side == MarketSide::Up)
            .collect();
        let down_orders: Vec<LadderOrder> = plan
            .orders
            .iter()
            .cloned()
            .filter(|o| o.side == MarketSide::Down)
            .collect();

        if up_orders.is_empty() || down_orders.is_empty() {
            return;
        }

        let imbalance = (position.up_shares - position.down_shares).abs();
        let cheap_orders = if cheap_is_up {
            up_orders.clone()
        } else {
            down_orders.clone()
        };
        let expensive_orders = if cheap_is_up {
            down_orders.clone()
        } else {
            up_orders.clone()
        };
        let (cheap_ask, expensive_ask) = if cheap_is_up {
            (up_ask, down_ask)
        } else {
            (down_ask, up_ask)
        };
        let (cheap_momentum, expensive_momentum) = if cheap_is_up {
            (up.momentum, down.momentum)
        } else {
            (down.momentum, up.momentum)
        };

        if allow_expensive
            && expensive_ask >= EXTREME_HIGH_PRICE
            && cheap_ask <= EXTREME_LOW_PRICE
            && expensive_momentum >= EXTREME_MOMENTUM_THRESHOLD
            && cheap_momentum <= -EXTREME_MOMENTUM_THRESHOLD
        {
            plan.orders = expensive_orders;
            return;
        }

        if imbalance >= (self.config.order_size * dec!(2.0)) {
            let (leading, lagging, lagging_orders) = if position.up_shares >= position.down_shares {
                (position.up_shares, position.down_shares, down_orders)
            } else {
                (position.down_shares, position.up_shares, up_orders)
            };

            let max_exposure = self.arb_finder.max_exposure_per_market();
            let max_add = leading.min(max_exposure) - lagging;
            if max_add > Decimal::ZERO {
                let equalize = cap_orders_to_size(lagging_orders, max_add);
                if !equalize.is_empty() {
                    plan.orders = equalize;
                    return;
                }
            }
        }

        if imbalance < self.config.order_size {
            plan.orders = cheap_orders;
            return;
        }

        plan.orders = cheap_orders;
    }

    fn should_allow_expensive(
        &self,
        cheap_ask: Decimal,
        expensive_ask: Decimal,
        expensive_cap: Decimal,
        imbalance: Decimal,
        time_to_end: chrono::Duration,
        prediction_confidence: Option<Decimal>,
    ) -> bool {
        if expensive_ask <= expensive_cap {
            return true;
        }

        let hedge_loss = (expensive_ask - expensive_cap).max(Decimal::ZERO);
        let min_imbalance = self.config.order_size * HEDGE_MIN_IMBALANCE_MULT;
        let in_end_window = time_to_end
            <= chrono::Duration::from_std(HEDGE_TIME_WINDOW)
                .unwrap_or_else(|_| chrono::Duration::zero());

        if hedge_loss <= HEDGE_MAX_LOSS && imbalance >= min_imbalance && in_end_window {
            return true;
        }

        if let Some(confidence) = prediction_confidence {
            let extra = (confidence * dec!(0.01)).min(HEDGE_MAX_LOSS);
            if hedge_loss <= (HEDGE_MAX_LOSS + extra) && imbalance >= min_imbalance {
                return true;
            }
        }

        false
    }
    fn equalize_cap(&self, current_up: Decimal, current_down: Decimal) -> (Decimal, Decimal) {
        let max_exposure = self.arb_finder.max_exposure_per_market();

        if current_up >= current_down {
            let max_add_down = current_up.min(max_exposure) - current_down;
            (Decimal::ZERO, max_add_down.max(Decimal::ZERO))
        } else {
            let max_add_up = current_down.min(max_exposure) - current_up;
            (max_add_up.max(Decimal::ZERO), Decimal::ZERO)
        }
    }

    /// Execute cancellations and place new orders
    async fn execute_action(&mut self, market: &ActiveMarket, action: MarketAction) {
        let market_id = &market.market_id;

        match action {
            MarketAction::Arb {
                up_ask,
                down_ask,
                combined,
                up_size: _,
                down_size: _,
            } => {
                // Get current position to check exposure limits
                let snapshot = self.ledger.get_state(market_id).await;
                let current_up = snapshot.position.up_shares;
                let current_down = snapshot.position.down_shares;

                let (size_cap_up, size_cap_down) = self.equalize_cap(current_up, current_down);

                // Calculate safe order size (use high confidence since this is a direct arb)
                let (safe_up_size, safe_down_size) = self.arb_finder.calculate_safe_arb_size(
                    current_up,
                    current_down,
                    dec!(0.9), // High confidence for direct arb
                    false,
                );

                let safe_up_size = safe_up_size.min(size_cap_up);
                let safe_down_size = safe_down_size.min(size_cap_down);

                if safe_up_size <= Decimal::ZERO || safe_down_size <= Decimal::ZERO {
                    info!(
                        market_id,
                        current_up = %current_up,
                        current_down = %current_down,
                        max_exposure = %self.arb_finder.max_exposure_per_market(),
                        "Skipping arb - max exposure reached"
                    );
                    return;
                }

                info!(
                    market_id,
                    up_ask = %up_ask,
                    down_ask = %down_ask,
                    combined = %combined,
                    up_size = %safe_up_size,
                    down_size = %safe_down_size,
                    current_up = %current_up,
                    current_down = %current_down,
                    "Executing arb orders with safe sizing"
                );

                let placed = self
                    .execute_arb_orders(market, up_ask, down_ask, safe_up_size, safe_down_size)
                    .await;

                info!(
                    market_id,
                    placed,
                    up_price = %up_ask,
                    down_price = %down_ask,
                    "Arb execution complete"
                );
            }
            MarketAction::Ladder {
                up_ask,
                down_ask,
                cancellations,
                orders,
            } => {
                let mut cancelled_count = 0;
                if !cancellations.is_empty() {
                    match self.cancel_orders(&cancellations).await {
                        Ok(cancelled) => {
                            cancelled_count = cancelled.len();
                            if cancelled_count > 0 {
                                self.ledger.mark_orders_cancelled(cancelled).await;
                            }
                        }
                        Err(e) => {
                            warn!(market_id, error = %e, "Failed to cancel orders");
                        }
                    }
                }

                // Place new orders
                if !orders.is_empty() {
                    let placed = self.place_orders(market, &orders).await;

                    if placed > 0 {
                        info!(
                            market_id,
                            up_ask = %up_ask,
                            down_ask = %down_ask,
                            cancelled = cancelled_count,
                            placed,
                            "Ladder updated"
                        );

                        self.ladder_state
                            .record_ladder(market_id.clone(), up_ask, down_ask);
                    }
                }
            }
        }

        self.last_order_by_market
            .insert(market_id.clone(), Instant::now());
    }

    async fn place_orders(&self, market: &ActiveMarket, orders: &[LadderOrder]) -> usize {
        let build_start = Instant::now();
        let signed = self.build_signed_orders(orders, market).await;
        let build_ms = build_start.elapsed().as_millis();

        if signed.is_empty() {
            return 0;
        }

        let post_start = Instant::now();
        match self.client.post_orders(signed).await {
            Ok(responses) => {
                let post_ms = post_start.elapsed().as_millis();
                debug!(
                    market_id = %market.market_id,
                    build_ms,
                    post_ms,
                    "Order batch timing"
                );
                let mut placed = 0;

                for (resp, order) in responses.iter().zip(orders.iter()) {
                    let success = resp
                        .error_msg
                        .as_ref()
                        .map(|s| s.is_empty())
                        .unwrap_or(true);

                    if success {
                        let token_id = match order.side {
                            MarketSide::Up => &market.up_token_id,
                            MarketSide::Down => &market.down_token_id,
                        };

                        self.ledger
                            .record_order_placed(
                                resp.order_id.clone(),
                                market.market_id.clone(),
                                token_id.clone(),
                                order.side,
                                order.size,
                                order.price,
                            )
                            .await;

                        placed += 1;
                    } else {
                        warn!(
                            market_id = %market.market_id,
                            order_id = %resp.order_id,
                            error = ?resp.error_msg,
                            "Order rejected"
                        );
                    }
                }

                placed
            }
            Err(e) => {
                error!(
                    market_id = %market.market_id,
                    error = %e,
                    "Batch order submission failed"
                );
                0
            }
        }
    }

    /// Execute arbitrage by submitting both Up and Down orders in parallel
    async fn execute_arb_orders(
        &self,
        market: &ActiveMarket,
        up_price: Decimal,
        down_price: Decimal,
        up_size: Decimal,
        down_size: Decimal,
    ) -> usize {
        let arb_start = Instant::now();

        // Create orders for both sides
        let orders = vec![
            LadderOrder {
                side: MarketSide::Up,
                price: up_price,
                size: up_size,
            },
            LadderOrder {
                side: MarketSide::Down,
                price: down_price,
                size: down_size,
            },
        ];

        // Build and sign orders in parallel
        let signed = self.build_arb_orders(&orders, market).await;
        let build_ms = arb_start.elapsed().as_millis();

        if signed.is_empty() {
            warn!(
                market_id = %market.market_id,
                "Failed to build arb orders"
            );
            return 0;
        }

        info!(
            market_id = %market.market_id,
            order_count = signed.len(),
            build_ms,
            "Submitting arb orders"
        );

        // Submit all orders in one batch
        let post_start = Instant::now();
        match self.client.post_orders(signed).await {
            Ok(responses) => {
                let post_ms = post_start.elapsed().as_millis();
                let total_ms = arb_start.elapsed().as_millis();

                let mut placed = 0;
                let mut placed_order_ids: Vec<String> = Vec::new();
                for (resp, order) in responses.iter().zip(orders.iter()) {
                    let success = resp
                        .error_msg
                        .as_ref()
                        .map(|s| s.is_empty())
                        .unwrap_or(true);

                    if success {
                        let token_id = match order.side {
                            MarketSide::Up => &market.up_token_id,
                            MarketSide::Down => &market.down_token_id,
                        };

                        self.ledger
                            .record_order_placed(
                                resp.order_id.clone(),
                                market.market_id.clone(),
                                token_id.clone(),
                                order.side,
                                order.size,
                                order.price,
                            )
                            .await;

                        info!(
                            market_id = %market.market_id,
                            order_id = %resp.order_id,
                            side = ?order.side,
                            price = %order.price,
                            size = %order.size,
                            "Arb order placed"
                        );

                        placed_order_ids.push(resp.order_id.clone());
                        placed += 1;
                    } else {
                        warn!(
                            market_id = %market.market_id,
                            order_id = %resp.order_id,
                            side = ?order.side,
                            error = ?resp.error_msg,
                            "Arb order rejected"
                        );
                    }
                }

                info!(
                    market_id = %market.market_id,
                    placed,
                    build_ms,
                    post_ms,
                    total_ms,
                    "Arb execution timing"
                );

                // If IOC isn't available, auto-cancel anything that lingers.
                if !self.config.dry_run && !placed_order_ids.is_empty() {
                    let client = self.client.clone();
                    let ledger = self.ledger.clone();
                    let market_id = market.market_id.clone();
                    let timeout_ms = self.config.arb_order_timeout_ms;

                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(timeout_ms)).await;

                        let order_id_refs: Vec<&str> =
                            placed_order_ids.iter().map(|s| s.as_str()).collect();

                        match client.cancel_orders(&order_id_refs).await {
                            Ok(response) => {
                                let cancelled_count = response.canceled.len();
                                let not_cancelled_count = response.not_canceled.len();

                                if cancelled_count > 0 {
                                    ledger.mark_orders_cancelled(response.canceled).await;
                                }
                                debug!(
                                    market_id = %market_id,
                                    cancelled = cancelled_count,
                                    not_cancelled = not_cancelled_count,
                                    "Auto-cancelled lingering arb orders"
                                );
                            }
                            Err(e) => {
                                warn!(market_id = %market_id, error = %e, "Auto-cancel failed");
                            }
                        }
                    });
                }

                placed
            }
            Err(e) => {
                error!(
                    market_id = %market.market_id,
                    error = %e,
                    "Arb order submission failed"
                );
                0
            }
        }
    }

    async fn build_signed_orders(
        &self,
        orders: &[LadderOrder],
        market: &ActiveMarket,
    ) -> Vec<SignedOrder> {
        let signer = self.signer.clone().with_chain_id(Some(POLYGON));
        let client = self.client.clone();
        let up_token_id = market.up_token_id.clone();
        let down_token_id = market.down_token_id.clone();

        stream::iter(orders.iter().cloned())
            .map(|order| {
                let client = client.clone();
                let signer = signer.clone();
                let up_token_id = up_token_id.clone();
                let down_token_id = down_token_id.clone();
                async move {
                    let token_id = match order.side {
                        MarketSide::Up => up_token_id.as_str(),
                        MarketSide::Down => down_token_id.as_str(),
                    };

                    let tick_size = match client.tick_size(token_id).await {
                        Ok(resp) => resp.minimum_tick_size.as_decimal(),
                        Err(e) => {
                            error!(error = %e, "Failed to fetch tick size");
                            return None;
                        }
                    };
                    let price = quantize_price(order.price, tick_size);

                    let (poly_price, poly_size) = match convert_order_params(price, order.size) {
                        Some(p) => p,
                        None => return None,
                    };

                    info!(
                        size = %poly_size,
                        price = %poly_price,
                        side = ?order.side,
                        "Building order"
                    );

                    let signable = match client
                        .limit_order()
                        .token_id(token_id)
                        .price(poly_price)
                        .size(poly_size)
                        .side(ClobSide::Buy)
                        .build()
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            error!(error = %e, "Failed to build order");
                            return None;
                        }
                    };

                    match client.sign(&signer, signable).await {
                        Ok(s) => Some(s),
                        Err(e) => {
                            error!(error = %e, "Failed to sign order");
                            None
                        }
                    }
                }
            })
            .buffer_unordered(SIGN_CONCURRENCY)
            .filter_map(|signed| async move { signed })
            .collect::<Vec<_>>()
            .await
    }

    /// Build arb orders with IOC-like behavior (short expiration)
    /// Orders are built in parallel for both Up and Down sides
    async fn build_arb_orders(
        &self,
        orders: &[LadderOrder],
        market: &ActiveMarket,
    ) -> Vec<SignedOrder> {
        let signer = self.signer.clone().with_chain_id(Some(POLYGON));
        let client = self.client.clone();
        let up_token_id = market.up_token_id.clone();
        let down_token_id = market.down_token_id.clone();

        // Build both orders in parallel for speed
        stream::iter(orders.iter().cloned())
            .map(|order| {
                let client = client.clone();
                let signer = signer.clone();
                let up_token_id = up_token_id.clone();
                let down_token_id = down_token_id.clone();
                async move {
                    let token_id = match order.side {
                        MarketSide::Up => up_token_id.as_str(),
                        MarketSide::Down => down_token_id.as_str(),
                    };

                    // Fetch tick size (should be cached by SDK)
                    let tick_size = match client.tick_size(token_id).await {
                        Ok(resp) => resp.minimum_tick_size.as_decimal(),
                        Err(e) => {
                            error!(error = %e, side = ?order.side, "Failed to fetch tick size for arb");
                            return None;
                        }
                    };
                    let price = quantize_price(order.price, tick_size);

                    let (poly_price, poly_size) = match convert_order_params(price, order.size) {
                        Some(p) => p,
                        None => return None,
                    };

                    debug!(
                        size = %poly_size,
                        price = %poly_price,
                        side = ?order.side,
                        "Building arb order"
                    );

                    // Build order - using standard limit order
                    // TODO: Add .time_in_force(TimeInForce::IOC) if SDK supports it
                    // For now, we rely on speed and will manually cancel unfilled orders
                    let signable = match client
                        .limit_order()
                        .token_id(token_id)
                        .price(poly_price)
                        .size(poly_size)
                        .side(ClobSide::Buy)
                        .build()
                        .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            error!(error = %e, side = ?order.side, "Failed to build arb order");
                            return None;
                        }
                    };

                    match client.sign(&signer, signable).await {
                        Ok(s) => Some(s),
                        Err(e) => {
                            error!(error = %e, side = ?order.side, "Failed to sign arb order");
                            None
                        }
                    }
                }
            })
            .buffer_unordered(2) // Both orders in parallel
            .filter_map(|signed| async move { signed })
            .collect::<Vec<_>>()
            .await
    }

    fn convert_order_params(&self, order: &LadderOrder) -> Option<(PolyDecimal, PolyDecimal)> {
        convert_order_params(order.price, order.size)
    }

    async fn cancel_orders(&self, order_ids: &[String]) -> anyhow::Result<Vec<String>> {
        if order_ids.is_empty() {
            return Ok(Vec::new());
        }

        if self.config.dry_run {
            info!(count = order_ids.len(), "Dry run: would cancel orders");
            return Ok(order_ids.to_vec());
        }

        let order_id_refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();

        match self.client.cancel_orders(&order_id_refs).await {
            Ok(response) => {
                info!(
                    requested = order_ids.len(),
                    cancelled = response.canceled.len(),
                    not_cancelled = response.not_canceled.len(),
                    "Batch cancel complete"
                );
                Ok(response.canceled)
            }
            Err(e) => {
                error!(error = %e, "Batch cancel failed");
                Err(e.into())
            }
        }
    }

    fn should_refresh_markets(&self) -> bool {
        if self.last_market_refresh.elapsed() < Duration::from_secs(30) {
            return false;
        }

        let now = Utc::now();
        let total_seconds = now.minute() * 60 + now.second();
        let position_in_window = total_seconds % 900;

        // Refresh in the 840-870 second window of each 15-min period
        (840..870).contains(&position_in_window)
    }

    fn cooldown_elapsed(&self, market_id: &str) -> bool {
        self.last_order_by_market
            .get(market_id)
            .map(|t| t.elapsed() >= Duration::from_secs(self.config.cooldown_secs))
            .unwrap_or(true)
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn markets(&self) -> &HashMap<String, ActiveMarket> {
        &self.markets
    }
}

/// Encapsulates what needs to happen for a market
enum MarketAction {
    /// Normal ladder maintenance
    Ladder {
        up_ask: Decimal,
        down_ask: Decimal,
        cancellations: Vec<String>,
        orders: Vec<LadderOrder>,
    },
    /// Arbitrage opportunity detected - buy both sides immediately
    Arb {
        up_ask: Decimal,
        down_ask: Decimal,
        combined: Decimal,
        up_size: Decimal,
        down_size: Decimal,
    },
}

struct SideMetrics {
    momentum: Decimal,
    volatility: Decimal,
    depth: Decimal,
    size: Decimal,
    priority: Decimal,
}

fn convert_order_params(price: Decimal, size: Decimal) -> Option<(PolyDecimal, PolyDecimal)> {
    let price = PolyDecimal::try_from(price.to_string().as_str())
        .map_err(|e| error!(error = %e, "Invalid price format"))
        .ok()?;

    let size = PolyDecimal::try_from(size.to_string().as_str())
        .map_err(|e| error!(error = %e, "Invalid size format"))
        .ok()?;

    Some((price, size))
}

fn quantize_price(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price;
    }
    let ticks = (price / tick_size).floor();
    ticks * tick_size
}

fn clamp_decimal(value: Decimal, min: Decimal, max: Decimal) -> Decimal {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

fn round_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    let scaled = (value / step).round_dp(0);
    scaled * step
}

fn cap_orders_to_size(orders: Vec<LadderOrder>, max_size: Decimal) -> Vec<LadderOrder> {
    let mut remaining = max_size;
    let mut kept = Vec::new();

    for mut order in orders {
        if remaining <= Decimal::ZERO {
            break;
        }

        let size = round_to_step(order.size.min(remaining), SIZE_STEP);
        if size < DYNAMIC_MIN_ORDER_SIZE {
            continue;
        }

        order.size = size;
        kept.push(order);
        remaining -= size;
    }

    kept
}
