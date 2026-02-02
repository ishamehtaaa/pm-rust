// bot.rs - Simplified continuous trading bot

use crate::aggregate_tracker::AggregateTracker;
use crate::config::{Config, ASSETS_BY_NAME};
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderOverrides, LadderState};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};
use crate::poller::{InventoryLedger, MarketSide, spawn_order_feed};
use crate::price_feed::{PriceCache, spawn_price_feed};
use crate::trend_window::TrendWindow;

use alloy::primitives::U256;
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{Timelike, Utc};
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::{
    AssetType, OrderStatusType, Side as ClobSide, SignatureType, SignedOrder,
};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, B256, Decimal as PolyDecimal};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;
type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(50);
const BALANCE_REFRESH_SECS: u64 = 10; // Faster refresh for better inventory tracking
const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";

pub struct SimpleBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    last_market_refresh: Instant,
    last_order_by_market: HashMap<String, Instant>,
    last_order_status_check: HashMap<String, Instant>,
    last_balance_refresh: HashMap<String, Instant>,

    ledger: Arc<RwLock<InventoryLedger>>,
    price_cache: Arc<RwLock<PriceCache>>,
    _order_feed: Option<tokio::task::JoinHandle<()>>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    active_market_ids: Arc<RwLock<Vec<String>>>,
    ws_client: Arc<AuthenticatedWsClient>,

    ladder_engine: LadderEngine,
    ladder_state: LadderState,
    aggregate_tracker: AggregateTracker,
    last_aggregate_log: HashMap<String, Instant>,
    last_missing_price_log: HashMap<String, Instant>,
    last_dry_run_log: HashMap<String, Instant>,
    
    // Trend analysis for smarter order placement
    trend_windows: HashMap<String, TrendWindow>,
    last_trend_log: HashMap<String, Instant>,
    
    // Time-window based accumulation tracking
    window_state: HashMap<String, WindowAccumulation>,
}

/// Tracks accumulation within a time window for balanced buying
#[derive(Debug, Clone, Default)]
pub struct WindowAccumulation {
    pub window_start_ms: i64,
    /// Position at window start (to calculate accumulation)
    pub up_at_start: Decimal,
    pub down_at_start: Decimal,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let market_cache = MarketCache::new(config.target_assets.clone(), config.target_duration);
        let price_cache = Arc::new(RwLock::new(PriceCache::default()));

        // Build ladder config from trading config
        let ladder_tuning = config.ladder();
        let mut ladder_config = LadderConfig::default();
        ladder_config.size_per_level = ladder_tuning.order_size;
        ladder_config.target_per_side = ladder_tuning.shares_target_per_side;
        ladder_config.max_pair_cost = ladder_tuning.target_total_cost;
        ladder_config.top_offset = ladder_tuning.maker_price_offset;
        ladder_config.levels = ladder_tuning.levels;
        ladder_config.reladder_threshold = ladder_tuning.reladder_threshold;
        ladder_config.stale_order_distance = ladder_tuning.stale_order_distance;
        ladder_config.tick_size = config.tick_size;
        ladder_config.max_imbalance = ladder_tuning.max_imbalance_shares;
        ladder_config.allow_single_side = ladder_tuning.allow_single_side;
        ladder_config.aggressive_rebalance_threshold = ladder_tuning.aggressive_rebalance_threshold;
        ladder_config.taker_threshold = ladder_tuning.taker_threshold;
        ladder_config.taker_size_multiplier = ladder_tuning.taker_size_multiplier;
        
        let ladder_engine = LadderEngine::new(ladder_config);
        let ladder_state = LadderState::default();
        let aggregate_tracker = AggregateTracker::default();

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));

        let addr = Address::from_str(config.polymarket_proxy_address.trim())
            .map_err(|e| anyhow::anyhow!("Invalid POLYMARKET_PROXY_ADDRESS: {}", e))?;

        let rest_client =
            Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?;
        let credentials: Credentials = rest_client
            .create_or_derive_api_key(&signer_with_chain, None)
            .await?;

        let client = Arc::new(
            rest_client
                .authentication_builder(&signer_with_chain)
                .credentials(credentials.clone())
                .funder(addr)
                .signature_type(SignatureType::Proxy)
                .authenticate()
                .await?,
        );

        let ws_client = Arc::new(
            WsClient::new(WS_SUB_URL, Default::default())?.authenticate(credentials, addr)?,
        );

        let ledger = Arc::new(RwLock::new(InventoryLedger::default()));
        let active_market_ids = Arc::new(RwLock::new(Vec::new()));

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            last_order_status_check: HashMap::new(),
            last_balance_refresh: HashMap::new(),
            active_market_ids,
            ledger,
            _order_feed: None,
            ws_client,
            price_cache,
            _price_feed: None,
            ladder_engine,
            ladder_state,
            aggregate_tracker,
            last_aggregate_log: HashMap::new(),
            last_missing_price_log: HashMap::new(),
            last_dry_run_log: HashMap::new(),
            trend_windows: HashMap::new(),
            last_trend_log: HashMap::new(),
            window_state: HashMap::new(),
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;
        if self.market_count() == 0 {
            warn!("No markets discovered, will retry in main loop");
        } else {
            for state in self.markets.values() {
                let symbol = market_symbol(&state.info);
                info!("Market: {} | {}", symbol, state.info.duration);
            }
        }
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

    async fn refresh_positions(
        &mut self,
        market_id: &str,
        up_token_id: &str,
        down_token_id: &str,
        market_symbol: &str,
    ) {
        let now = Instant::now();
        let should_refresh = match self.last_balance_refresh.get(market_id) {
            Some(last) => now.duration_since(*last) >= Duration::from_secs(BALANCE_REFRESH_SECS),
            None => true,
        };

        if !should_refresh {
            return;
        }

        self.last_balance_refresh.insert(market_id.to_string(), now);

        match self.fetch_token_balances(up_token_id, down_token_id).await {
            Ok((up_bal, down_bal)) => {
                self.ledger
                    .write()
                    .set_initial_position(market_id.to_string(), up_bal, down_bal);
                debug!(
                    market = %market_symbol,
                    up_shares = %up_bal,
                    down_shares = %down_bal,
                    "Position refreshed"
                );
            }
            Err(e) => {
                warn!(
                    market = %market_symbol,
                    error = %e,
                    "Failed to refresh position"
                );
            }
        }
    }

    async fn fetch_token_balances(
        &self,
        up_token_id: &str,
        down_token_id: &str,
    ) -> anyhow::Result<(Decimal, Decimal)> {
        let up_token = U256::from_str(up_token_id)
            .map_err(|e| anyhow::anyhow!("Invalid up token id {}: {}", up_token_id, e))?;
        let down_token = U256::from_str(down_token_id)
            .map_err(|e| anyhow::anyhow!("Invalid down token id {}: {}", down_token_id, e))?;

        let up_req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Conditional)
            .token_id(up_token)
            .build();

        let down_req = BalanceAllowanceRequest::builder()
            .asset_type(AssetType::Conditional)
            .token_id(down_token)
            .build();

        let up_resp = self.client.balance_allowance(up_req).await?;
        let down_resp = self.client.balance_allowance(down_req).await?;

        let up_bal = up_resp.balance / dec!(1_000_000);
        let down_bal = down_resp.balance / dec!(1_000_000);

        Ok((up_bal, down_bal))
    }

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");
        
        let previous_markets: Vec<(String, String, String)> = self
            .markets
            .iter()
            .map(|(market_id, state)| {
                (
                    market_id.clone(),
                    state.info.up_token_id.clone(),
                    state.info.down_token_id.clone(),
                )
            })
            .collect();

        let all_markets = match self.market_cache.get_markets(Utc::now()).await {
            Ok(markets) => markets,
            Err(e) => {
                error!(error = %e, "Failed to fetch markets");
                return;
            }
        };

        let now = Utc::now();
        let active_markets: Vec<_> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

        // Select nearest market per asset
        let mut selected: HashMap<String, MarketInfo> = HashMap::new();
        for market in active_markets {
            match selected.entry(market.asset.clone()) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if market.end_time < entry.get().end_time {
                        entry.insert(market);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(market);
                }
            }
        }

        self.markets.clear();
        self.trading_pairs.clear();

        for (asset, info) in selected {
            let market_id = info.id.clone();
            let market_sym = market_symbol(&info);
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                start_time: info.start_time,
                end_time: info.end_time,
            };

            info!(
                market = %market_sym,
                asset = %asset,
                end_time = %state.end_time,
                "Market registered"
            );

            self.markets.insert(market_id.clone(), state);
            self.trading_pairs.insert(market_id.clone(), pair);
            self.ledger.write().register_market(market_id, market_sym);
        }

        // Clean up removed markets
        for (market_id, _, _) in previous_markets.iter() {
            if !self.markets.contains_key(market_id) {
                self.ladder_state.clear_market(market_id);
                self.aggregate_tracker.remove_market(market_id);
                self.last_aggregate_log.remove(market_id);
                self.last_missing_price_log.remove(market_id);
                self.last_dry_run_log.remove(market_id);
                self.last_order_by_market.remove(market_id);
                self.trend_windows.remove(market_id);
                self.last_trend_log.remove(market_id);
                self.window_state.remove(market_id);
            }
        }

        // Cancel orders for removed markets
        let removed_market_ids: Vec<String> = previous_markets
            .iter()
            .filter(|(id, _, _)| !self.markets.contains_key(id))
            .map(|(id, _, _)| id.clone())
            .collect();

        if !removed_market_ids.is_empty() {
            let cancel_ids: Vec<String> = {
                let ledger = self.ledger.read();
                removed_market_ids
                    .iter()
                    .flat_map(|market_id| {
                        ledger
                            .open_orders_for_market(market_id)
                            .into_iter()
                            .map(|o| o.order_id)
                    })
                    .collect()
            };

            if !cancel_ids.is_empty() {
                match self.cancel_orders(&cancel_ids).await {
                    Ok(cancelled) => {
                        info!(count = cancelled.len(), "Cancelled orders for expired markets");
                        self.ledger.write().mark_orders_cancelled(&cancelled);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to cancel orders for expired markets");
                    }
                }
            }

            let mut ledger = self.ledger.write();
            for market_id in removed_market_ids {
                ledger.unregister_market(&market_id);
            }
        }

        // Update active market IDs
        {
            let mut ids = self.active_market_ids.write();
            *ids = self.markets.keys().cloned().collect();
        }

        // Fetch initial positions
        for (market_id, state) in &self.markets {
            let market_sym = market_symbol(&state.info);
            match self
                .fetch_token_balances(&state.info.up_token_id, &state.info.down_token_id)
                .await
            {
                Ok((up_bal, down_bal)) => {
                    self.ledger
                        .write()
                        .set_initial_position(market_id.clone(), up_bal, down_bal);
                    info!(
                        market = %market_sym,
                        up_shares = %up_bal,
                        down_shares = %down_bal,
                        "Position initialized"
                    );
                }
                Err(e) => {
                    warn!(market = %market_sym, error = %e, "Failed to fetch initial position");
                }
            }
        }

        // Start order feed
        if let Some(handle) = self._order_feed.take() {
            handle.abort();
        }

        let order_market_ids: Vec<B256> = self
            .markets
            .values()
            .filter_map(|state| state.info.condition_id)
            .collect();

        if !order_market_ids.is_empty() {
            self._order_feed = Some(spawn_order_feed(
                self.ws_client.clone(),
                self.ledger.clone(),
                order_market_ids,
            ));
        }

        // Start price feed
        if let Some(handle) = self._price_feed.take() {
            handle.abort();
        }

        let token_ids: Vec<String> = self
            .markets
            .values()
            .flat_map(|m| vec![m.info.up_token_id.clone(), m.info.down_token_id.clone()])
            .collect();

        match spawn_price_feed(WS_SUB_URL, token_ids, self.price_cache.clone()) {
            Ok(handle) => {
                self._price_feed = Some(handle);
                info!("Price feed started");
            }
            Err(e) => {
                error!(error = %e, "Failed to start price feed");
            }
        }
    }

    fn should_refresh_markets(&self) -> bool {
        if self.last_market_refresh.elapsed() < Duration::from_secs(30) {
            return false;
        }

        let now = Utc::now();
        if self.markets.values().any(|m| m.end_time <= now) {
            return true;
        }

        // Refresh near the end of each 15-minute window
        let total_seconds = now.minute() * 60 + now.second();
        let position_in_window = total_seconds % 900;
        (840..870).contains(&position_in_window)
    }

    fn cooldown_elapsed(&self, market_id: &str) -> bool {
        match self.last_order_by_market.get(market_id) {
            Some(t) => t.elapsed() >= Duration::from_secs(self.config.cooldown_secs),
            None => true,
        }
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    /// Main scan loop - simplified for continuous trading
    async fn scan(&mut self) {
        let market_ids: Vec<String> = self.markets.keys().cloned().collect();

        for market_id in market_ids {
            // Extract all needed values from state first to avoid borrow conflicts
            let (market_sym, up_token_id, down_token_id) = {
                let state = match self.markets.get(&market_id) {
                    Some(s) => s,
                    None => continue,
                };
                let market_sym = market_symbol(&state.info);
                let up_token_id = state.info.up_token_id.clone();
                let down_token_id = state.info.down_token_id.clone();
                (market_sym, up_token_id, down_token_id)
            };

            // Refresh positions periodically
            self.refresh_positions(&market_id, &up_token_id, &down_token_id, &market_sym)
                .await;

            // Sync aggregate tracker from ledger
            self.sync_aggregate_from_ledger(&market_id);

            // Log aggregate position periodically
            if self.should_log_aggregate(&market_id) {
                self.log_aggregate_position(&market_id, &market_sym);
            }

            // Get current prices
            let price_result = {
                let cache = self.price_cache.read();
                let now_ms = Utc::now().timestamp_millis() as u64;
                
                match (
                    cache.get_with_age(&up_token_id, now_ms),
                    cache.get_with_age(&down_token_id, now_ms),
                ) {
                    (Some((ub, ua, up_age)), Some((db, da, down_age))) => {
                        let max_age = up_age.max(down_age) as i64;
                        if max_age > self.config.max_price_age_ms {
                            Err(format!("stale:{}", max_age))
                        } else {
                            Ok((ub, ua, db, da))
                        }
                    }
                    _ => Err("missing".to_string()),
                }
            };

            let (_up_bid, up_ask, _down_bid, down_ask) = match price_result {
                Ok(prices) => prices,
                Err(reason) => {
                    if self.should_log_missing_price(&market_id) {
                        debug!(market = %market_sym, reason = %reason, "Price data unavailable");
                    }
                    continue;
                }
            };

            // ===== SIMPLE STRATEGY: ALWAYS IN THE BOOK =====
            // Like gabagool - maintain orders on BOTH sides at all times
            // Calculate MAX price we can pay based on our existing avg on other side
            
            // Get current position
            let pos = {
                let ledger = self.ledger.read();
                ledger.effective_position(&market_id)
            };
            
            // Get our current averages
            let (up_avg, down_avg) = {
                let agg = self.aggregate_tracker.get(&market_id);
                match agg {
                    Some(a) => (a.up_avg_price(), a.down_avg_price()),
                    None => (None, None),
                }
            };
            
            // Calculate max prices we can pay while staying profitable
            // max_up = 1.0 - down_avg (so combined stays <= 1.0)
            // max_down = 1.0 - up_avg
            let max_up_price = match down_avg {
                Some(d_avg) => (dec!(1.0) - d_avg).max(dec!(0.01)),
                None => dec!(0.99), // No down yet, can pay up to 0.99
            };
            
            let max_down_price = match up_avg {
                Some(u_avg) => (dec!(1.0) - u_avg).max(dec!(0.01)),
                None => dec!(0.99), // No up yet, can pay up to 0.99
            };
            
            // Calculate imbalance
            let imbalance = pos.up_shares - pos.down_shares;
            let up_behind = imbalance < Decimal::ZERO;
            let down_behind = imbalance > Decimal::ZERO;
            
            // Log state periodically
            if self.should_log_trend(&market_id) {
                debug!(
                    market = %market_sym,
                    up_ask = %up_ask,
                    max_up = %format!("{:.2}", max_up_price),
                    down_ask = %down_ask,
                    max_down = %format!("{:.2}", max_down_price),
                    up_avg = ?up_avg,
                    down_avg = ?down_avg,
                    imbalance = %format!("{:.1}", imbalance),
                    "Always in book"
                );
            }
            
            // Check if we should reladder
            let should_reladder = self.ladder_state.should_reladder(
                &market_id,
                up_ask,
                down_ask,
                self.ladder_engine.config().reladder_threshold,
            );

            if !should_reladder && !self.cooldown_elapsed(&market_id) {
                continue;
            }

            // Get open orders (we already have pos from above)
            let open_orders = {
                let ledger = self.ledger.read();
                ledger.open_orders_for_market(&market_id)
            };

            // Build ladder overrides
            let mut overrides = LadderOverrides::default();
            
            // Prioritize the side that's behind to stay balanced
            if up_behind {
                overrides.allow_imbalance_side = Some(MarketSide::Up);
            } else if down_behind {
                overrides.allow_imbalance_side = Some(MarketSide::Down);
            }
            
            // Set price caps based on what keeps us profitable
            // Place orders up to max price - we stay in the book at good prices
            overrides.up_price_cap = Some(max_up_price.min(up_ask));
            overrides.down_price_cap = Some(max_down_price.min(down_ask));

            // Compute ladder plan
            let target = self.config.trading.target_per_side;
            let plan = self.ladder_engine.compute_ladder_with_target(
                up_ask,
                down_ask,
                &pos,
                &open_orders,
                Some(target),
                Some(overrides),
            );

            if plan.cancellations.is_empty() && plan.orders.is_empty() {
                continue;
            }

            // Cancel stale orders
            if !plan.cancellations.is_empty() {
                match self.cancel_orders(&plan.cancellations).await {
                    Ok(cancelled) => {
                        if !cancelled.is_empty() {
                            debug!(
                                market = %market_sym,
                                count = cancelled.len(),
                                "Cancelled stale orders"
                            );
                        }
                        self.ledger.write().mark_orders_cancelled(&cancelled);
                    }
                    Err(e) => {
                        error!(market = %market_sym, error = %e, "Failed to cancel orders");
                    }
                }
            }

            // Place new orders
            if !plan.orders.is_empty() {
                if self.config.dry_run {
                    if self.should_log_dry_run(&market_id) {
                        let combined = up_ask + down_ask;
                        let taker_threshold = self.config.trading.taker_threshold;
                        let mode = if combined < taker_threshold { "TAKER" } else { "maker" };
                        let edge_pct = (rust_decimal_macros::dec!(1) - combined) * rust_decimal_macros::dec!(100);
                        info!(
                            market = %market_sym,
                            mode = %mode,
                            count = plan.orders.len(),
                            up_ask = %up_ask,
                            down_ask = %down_ask,
                            combined = %format!("{:.3}", combined),
                            edge = %format!("{:.2}%", edge_pct),
                            "Dry run: would place orders"
                        );
                        for order in &plan.orders {
                            debug!(
                                side = ?order.side,
                                price = %order.price,
                                size = %order.size,
                                "  Order"
                            );
                        }
                    }
                    continue;
                }

                let signed_orders = self
                    .build_signed_orders(&plan.orders, &up_token_id, &down_token_id)
                    .await;

                if !signed_orders.is_empty() {
                    match self.client.post_orders(signed_orders).await {
                        Ok(responses) => {
                            let mut placed = 0;
                            for (resp, order) in responses.iter().zip(plan.orders.iter()) {
                                if resp.error_msg.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                                    self.ledger.write().record_order_placed(
                                        resp.order_id.clone(),
                                        market_id.clone(),
                                        order.side,
                                        order.size,
                                        order.price,
                                    );
                                    placed += 1;
                                } else {
                                    warn!(
                                        market = %market_sym,
                                        order_id = %resp.order_id,
                                        error = ?resp.error_msg,
                                        "Order rejected"
                                    );
                                }
                            }

                            if placed > 0 {
                                let combined = up_ask + down_ask;
                                let taker_threshold = self.config.trading.taker_threshold;
                                let mode = if combined < taker_threshold { "TAKER" } else { "maker" };
                                let edge_pct = (rust_decimal_macros::dec!(1) - combined) * rust_decimal_macros::dec!(100);
                                info!(
                                    market = %market_sym,
                                    mode = %mode,
                                    up_ask = %up_ask,
                                    down_ask = %down_ask,
                                    combined = %format!("{:.3}", combined),
                                    edge = %format!("{:.2}%", edge_pct),
                                    placed,
                                    "Orders placed"
                                );
                            }

                            self.ladder_state.record_ladder(market_id.clone(), up_ask, down_ask);
                        }
                        Err(e) => {
                            error!(market = %market_sym, error = %e, "Order submission failed");
                        }
                    }
                }
            }

            self.last_order_by_market.insert(market_id.clone(), Instant::now());
        }
    }

    fn should_log_missing_price(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        match self.last_missing_price_log.get(market_id) {
            Some(last) if now.duration_since(*last) < Duration::from_secs(5) => false,
            _ => {
                self.last_missing_price_log.insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_dry_run(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        match self.last_dry_run_log.get(market_id) {
            Some(last) if now.duration_since(*last) < Duration::from_secs(10) => false,
            _ => {
                self.last_dry_run_log.insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_aggregate(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        match self.last_aggregate_log.get(market_id) {
            Some(last) if now.duration_since(*last) < Duration::from_secs(10) => false,
            _ => {
                self.last_aggregate_log.insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_trend(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        match self.last_trend_log.get(market_id) {
            Some(last) if now.duration_since(*last) < Duration::from_secs(15) => false,
            _ => {
                self.last_trend_log.insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn sync_aggregate_from_ledger(&mut self, market_id: &str) {
        let (up_shares, up_cost, down_shares, down_cost) =
            self.ledger.read().aggregate_for_market(market_id);

        let agg = self.aggregate_tracker.get_mut(market_id);
        agg.up_shares = up_shares;
        agg.up_total_cost = up_cost;
        agg.down_shares = down_shares;
        agg.down_total_cost = down_cost;
    }

    fn log_aggregate_position(&self, market_id: &str, market_sym: &str) {
        let Some(agg) = self.aggregate_tracker.get(market_id) else {
            return;
        };

        if agg.up_shares.is_zero() && agg.down_shares.is_zero() {
            return;
        }

        let up_avg = agg.up_avg_price().unwrap_or_default();
        let down_avg = agg.down_avg_price().unwrap_or_default();
        let imbalance = agg.imbalance();
        let paired = agg.paired_shares();

        match agg.combined_avg_cost() {
            Some(cost) => {
                let edge_pct = (dec!(1) - cost) * dec!(100);
                let profit = agg.guaranteed_profit().unwrap_or_default();
                info!(
                    market = %market_sym,
                    up = %format!("{:.1}@{:.3}", agg.up_shares, up_avg),
                    down = %format!("{:.1}@{:.3}", agg.down_shares, down_avg),
                    combined = %format!("{:.4}", cost),
                    edge = %format!("{:.2}%", edge_pct),
                    imbalance = %imbalance,
                    paired = %paired,
                    profit = %format!("${:.2}", profit),
                    "Aggregate"
                );
            }
            None => {
                debug!(
                    market = %market_sym,
                    up_shares = %agg.up_shares,
                    down_shares = %agg.down_shares,
                    "Aggregate (one-sided)"
                );
            }
        }
    }

    async fn build_signed_orders(
        &self,
        orders: &[LadderOrder],
        up_token_id: &str,
        down_token_id: &str,
    ) -> Vec<SignedOrder> {
        let mut signed = Vec::with_capacity(orders.len());

        for order in orders {
            let token_id = match order.side {
                MarketSide::Up => up_token_id,
                MarketSide::Down => down_token_id,
            };
            let token_id = match U256::from_str(token_id) {
                Ok(id) => id,
                Err(e) => {
                    error!(error = %e, token_id = %token_id, "Invalid token id");
                    continue;
                }
            };

            let poly_price = match PolyDecimal::try_from(order.price.to_string().as_str()) {
                Ok(p) => p,
                Err(e) => {
                    error!(error = %e, "Invalid price format");
                    continue;
                }
            };

            let poly_size = match PolyDecimal::try_from(order.size.to_string().as_str()) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "Invalid size format");
                    continue;
                }
            };

            debug!(
                side = ?order.side,
                price = %poly_price,
                size = %poly_size,
                "Building order"
            );

            let signable = match self
                .client
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
                    continue;
                }
            };

            match self
                .client
                .sign(&self.signer.clone().with_chain_id(Some(POLYGON)), signable)
                .await
            {
                Ok(s) => signed.push(s),
                Err(e) => {
                    error!(error = %e, "Failed to sign order");
                }
            }
        }

        signed
    }

    async fn cancel_orders(&mut self, order_ids: &[String]) -> anyhow::Result<Vec<String>> {
        if order_ids.is_empty() {
            return Ok(Vec::new());
        }

        if self.config.dry_run {
            debug!(count = order_ids.len(), "Dry run: would cancel orders");
            return Ok(order_ids.to_vec());
        }

        let order_id_refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();

        match self.client.cancel_orders(&order_id_refs).await {
            Ok(response) => {
                if !response.not_canceled.is_empty() {
                    // Check status of orders that couldn't be cancelled
                    for (order_id, _reason) in &response.not_canceled {
                        let should_check = match self.last_order_status_check.get(order_id) {
                            Some(last) => last.elapsed() >= Duration::from_secs(10),
                            None => true,
                        };

                        if should_check {
                            self.last_order_status_check
                                .insert(order_id.clone(), Instant::now());

                            if let Ok(order) = self.client.order(order_id).await {
                                let is_live = matches!(order.status, OrderStatusType::Live);
                                self.ledger.write().apply_order_status(
                                    order_id,
                                    order.size_matched,
                                    is_live,
                                );
                            }
                        }
                    }
                }

                Ok(response.canceled)
            }
            Err(e) => {
                error!(error = %e, "Cancel orders failed");
                Err(e.into())
            }
        }
    }
}

fn market_symbol(info: &MarketInfo) -> String {
    if let Some(prefix) = info.slug.split('-').next() {
        return prefix.to_string();
    }
    ASSETS_BY_NAME
        .get(&info.asset)
        .and_then(|asset| asset.prefixes.first())
        .cloned()
        .unwrap_or_else(|| info.asset.clone())
}
