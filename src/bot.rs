use crate::arb_finder::{
    ArbFinder, ArbFinderConfig, MarketInfo as ArbMarketInfo, RecommendedAction, spawn_trade_poller,
};
use crate::clob_api::fetch_token_balances;
use crate::config::Config;
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderState};
use crate::market_cache::MarketCache;
use crate::models::MarketInfo;
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
use polymarket_client_sdk::clob::types::{OrderType, Side as ClobSide, SignatureType, SignedOrder};
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

const LOOP_DELAY: Duration = Duration::from_millis(10);  // Fast loop for arb detection
const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";
const PREFETCH_CONCURRENCY: usize = 6;
const SIGN_CONCURRENCY: usize = 4;

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

    /// Sync cache for tick sizes (avoids HTTP calls in hot path)
    tick_size_cache: Arc<RwLock<HashMap<String, Decimal>>>,

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
            tick_size_cache: Arc::new(RwLock::new(HashMap::new())),
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
            })
            .collect();
        self.arb_finder.init_markets(arb_markets);

        // Start trade poller for sweep detection
        let trade_markets: Vec<_> = markets
            .iter()
            .map(|m| {
                (
                    m.market_id.clone(),
                    m.up_token_id.clone(),
                    m.down_token_id.clone(),
                )
            })
            .collect();
        
        let trade_poller_handle = spawn_trade_poller(
            self.arb_finder.config().clone(),
            self.client.clone(),
            self.arb_finder.state_store().clone(),
            self.arb_finder.data_logger().clone(),
            trade_markets,
        );
        self.arb_finder.set_trade_poller(trade_poller_handle);
        info!("Trade poller started for sweep detection");
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

        // Filter active, dedupe by asset (keep first per asset)
        let mut by_asset: HashMap<String, ActiveMarket> = HashMap::new();
        for info in all_markets {
            if info.end_time > now {
                by_asset
                    .entry(info.asset.clone())
                    .or_insert_with(|| ActiveMarket::from_info(&info));
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
        let tick_cache = self.tick_size_cache.clone();
        
        stream::iter(token_ids)
            .map(|token_id| {
                let client = client.clone();
                let tick_cache = tick_cache.clone();
                async move {
                    // Fetch tick size and store in sync cache for fast lookup
                    if let Ok(resp) = client.tick_size(&token_id).await {
                        let tick_size = resp.minimum_tick_size.as_decimal();
                        tick_cache.write().insert(token_id.clone(), tick_size);
                        debug!(token_id = %token_id, tick_size = %tick_size, "Tick size cached");
                    }
                    let _ = client.neg_risk(&token_id).await;
                }
            })
            .buffer_unordered(PREFETCH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        
        info!(
            cache_size = self.tick_size_cache.read().len(),
            "Tick size cache populated"
        );
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
                    self.arb_finder.update_ws_price(&market.up_token_id, bid, ask);
                }
                if let Some((bid, ask)) = cache.get(&market.down_token_id) {
                    self.arb_finder.update_ws_price(&market.down_token_id, bid, ask);
                }
            }
        }

        // CROSS-PRODUCT ARB SCAN: Check full depth for opportunities at any price level
        // This logs all opportunities for analysis (per user request)
        for market in &markets {
            let opps = self.arb_finder.scan_cross_product_arbs(&market.market_id);
            if !opps.is_empty() {
                // Log summary - individual opportunities already logged in scan_cross_product_arbs
                debug!(
                    market_id = %market.market_id,
                    opportunity_count = opps.len(),
                    best_profit = %opps.first().map(|o| o.profit_per_pair).unwrap_or_default(),
                    "Cross-product arb scan complete"
                );
            }
        }

        // Check arb finder for predictions first
        let predictions = self.arb_finder.scan();
        for prediction in predictions {
            if prediction.recommended_action == RecommendedAction::ExecuteNow
                || prediction.recommended_action == RecommendedAction::PrePosition
            {
                // Get current position to calculate safe size
                if let Some(market) = self.markets.get(&prediction.market_id) {
                    let snapshot = self.ledger.get_state(&prediction.market_id).await;
                    let current_up = snapshot.position.up_shares;
                    let current_down = snapshot.position.down_shares;

                    // Calculate safe order size based on position and confidence
                    let (up_size, down_size) = self.arb_finder.calculate_safe_arb_size(
                        current_up,
                        current_down,
                        prediction.confidence,
                    );

                    // Only execute if we have room for orders
                    if up_size > Decimal::ZERO && down_size > Decimal::ZERO {
                        if let (Some(up_price), Some(down_price)) =
                            (prediction.up_target_price, prediction.down_target_price)
                        {
                            info!(
                                market_id = %prediction.market_id,
                                confidence = %prediction.confidence,
                                action = ?prediction.recommended_action,
                                up_size = %up_size,
                                down_size = %down_size,
                                current_up = %current_up,
                                current_down = %current_down,
                                "Executing arb with safe sizing"
                            );

                            let placed = self
                                .execute_arb_orders(market, up_price, down_price, up_size, down_size)
                                .await;

                            info!(
                                market_id = %prediction.market_id,
                                placed,
                                "Arb execution complete"
                            );
                        }
                    } else {
                        debug!(
                            market_id = %prediction.market_id,
                            current_up = %current_up,
                            current_down = %current_down,
                            max_exposure = %self.arb_finder.max_exposure_per_market(),
                            "Skipping arb - max exposure reached"
                        );
                    }
                }
            }
        }

        // Normal evaluation for each market
        for market in markets {
            if let Some(action) = self.evaluate_market(&market).await {
                self.execute_action(&market, action).await;
            }
        }
    }

    /// Evaluate a single market, returning an action if needed
    async fn evaluate_market(&self, market: &ActiveMarket) -> Option<MarketAction> {
        // Get prices
        let (up_ask, down_ask) = {
            let cache = self.price_cache.read();
            let up = cache.get(&market.up_token_id)?.1;
            let down = cache.get(&market.down_token_id)?.1;
            (up, down)
        };

        let combined = up_ask + down_ask;
        let spread_from_arb = combined - self.config.arb_threshold;

        // Log when spread is getting close to profitable (within 5%)
        if spread_from_arb < dec!(0.05) {
            debug!(
                market_id = %market.market_id,
                up_ask = %up_ask,
                down_ask = %down_ask,
                combined = %combined,
                arb_threshold = %self.config.arb_threshold,
                spread_from_arb = %spread_from_arb,
                "Spread check - getting close!"
            );
        } else {
            trace!(
                market_id = %market.market_id,
                up_ask = %up_ask,
                down_ask = %down_ask,
                combined = %combined,
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

        // Compute ladder
        let plan = self.ladder_engine.compute_ladder(
            up_ask,
            down_ask,
            &snapshot.position,
            snapshot.pending_up,
            snapshot.pending_down,
            &snapshot.open_orders,
        );

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

                // Calculate safe order size (use high confidence since this is a direct arb)
                let (safe_up_size, safe_down_size) = self.arb_finder.calculate_safe_arb_size(
                    current_up,
                    current_down,
                    dec!(0.9), // High confidence for direct arb
                );

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
                let cancelled_count = 0;
                if !cancellations.is_empty() {
                    debug!(
                        market_id,
                        count = cancellations.len(),
                        "Cancellations disabled"
                    );
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
        // Ensure sizes are rounded to 2 decimal places (Polymarket requirement)
        let orders = vec![
            LadderOrder {
                side: MarketSide::Up,
                price: up_price,
                size: up_size.round_dp(2),
            },
            LadderOrder {
                side: MarketSide::Down,
                price: down_price,
                size: down_size.round_dp(2),
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

                        placed += 1;
                    } else {
                        // FAK rejection means no liquidity at our price
                        // This is expected when the opportunity disappears before we execute
                        let error_msg = resp.error_msg.as_deref().unwrap_or("unknown");
                        let is_no_match = error_msg.contains("no orders found to match");
                        
                        if is_no_match {
                            info!(
                                market_id = %market.market_id,
                                side = ?order.side,
                                price = %order.price,
                                size = %order.size,
                                "FAK order found no liquidity at price (opportunity disappeared)"
                            );
                        } else {
                            warn!(
                                market_id = %market.market_id,
                                order_id = %resp.order_id,
                                side = ?order.side,
                                price = %order.price,
                                size = %order.size,
                                error = %error_msg,
                                "Arb order rejected"
                            );
                        }
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

    /// Build arb orders with FAK (Fill-And-Kill = IOC equivalent)
    /// Orders are built in parallel for both Up and Down sides
    /// Uses sync tick_size cache for fast execution
    async fn build_arb_orders(
        &self,
        orders: &[LadderOrder],
        market: &ActiveMarket,
    ) -> Vec<SignedOrder> {
        let signer = self.signer.clone().with_chain_id(Some(POLYGON));
        let client = self.client.clone();
        let up_token_id = market.up_token_id.clone();
        let down_token_id = market.down_token_id.clone();
        let tick_cache = self.tick_size_cache.clone();

        // Build both orders in parallel for speed
        stream::iter(orders.iter().cloned())
            .map(|order| {
                let client = client.clone();
                let signer = signer.clone();
                let up_token_id = up_token_id.clone();
                let down_token_id = down_token_id.clone();
                let tick_cache = tick_cache.clone();
                async move {
                    let token_id = match order.side {
                        MarketSide::Up => up_token_id.as_str(),
                        MarketSide::Down => down_token_id.as_str(),
                    };

                    // Use sync cache for tick size (fast path)
                    // Falls back to HTTP if not cached
                    let tick_size = {
                        let cache = tick_cache.read();
                        cache.get(token_id).copied()
                    }.unwrap_or_else(|| {
                        // Fallback: this shouldn't happen if prefetch worked
                        warn!(token_id = %token_id, "Tick size not in cache, will be fetched async");
                        dec!(0.01) // Default tick size
                    });
                    
                    let price = quantize_price(order.price, tick_size);

                    let (poly_price, poly_size) = match convert_order_params(price, order.size) {
                        Some(p) => p,
                        None => return None,
                    };

                    debug!(
                        size = %poly_size,
                        price = %poly_price,
                        side = ?order.side,
                        tick_size = %tick_size,
                        "Building arb order (fast path)"
                    );

                    // Build arb order with FAK (Fill-And-Kill = IOC equivalent)
                    // FAK fills as much as possible immediately, cancels any unfilled portion
                    let signable = match client
                        .limit_order()
                        .token_id(token_id)
                        .price(poly_price)
                        .size(poly_size)
                        .side(ClobSide::Buy)
                        .order_type(OrderType::FAK)
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

fn convert_order_params(price: Decimal, size: Decimal) -> Option<(PolyDecimal, PolyDecimal)> {
    // Round price to 2 decimal places (Polymarket requirement)
    let rounded_price = price.round_dp(2);
    let price = PolyDecimal::try_from(rounded_price.to_string().as_str())
        .map_err(|e| error!(error = %e, "Invalid price format"))
        .ok()?;

    // Round size to 2 decimal places (Polymarket requirement)
    let rounded_size = size.round_dp(2);
    
    // Skip if size is too small after rounding
    if rounded_size < dec!(0.01) {
        debug!(original_size = %size, "Size too small after rounding, skipping");
        return None;
    }
    
    let size = PolyDecimal::try_from(rounded_size.to_string().as_str())
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
