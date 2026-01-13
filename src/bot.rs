use crate::arb_finder::{
    ArbFinder, ArbFinderConfig, MarketInfo as ArbMarketInfo, RecommendedAction,
};
use crate::clob_api::fetch_token_balances;
use crate::config::Config;
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
    last_arb_attempt: HashMap<String, Instant>,

    /// Single source of truth for inventory
    ledger: LedgerHandle,

    /// Price data from WebSocket
    price_cache: Arc<RwLock<PriceCache>>,

    /// Background tasks
    _order_feed: Option<tokio::task::JoinHandle<()>>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    _reconciliation_poller: tokio::task::JoinHandle<()>,

    /// Cached tick sizes for fast order building (token_id -> tick_size)
    tick_size_cache: Arc<RwLock<HashMap<String, Decimal>>>,
    
    /// Cached neg_risk values for fast order building (token_id -> neg_risk)
    /// Pre-loaded at startup to avoid network calls during arb execution
    neg_risk_cache: Arc<RwLock<HashMap<String, bool>>>,

    /// Predictive arb finder with cross-product scanning
    arb_finder: ArbFinder,
    
    /// Track pending arb orders for timeout/cleanup
    pending_arb_orders: Arc<RwLock<HashMap<String, PendingArbOrder>>>,
}

/// Tracks a pending arb order for timeout cleanup
#[derive(Clone)]
#[allow(dead_code)]
struct PendingArbOrder {
    order_id: String,
    market_id: String,
    side: MarketSide,
    placed_at: Instant,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot (arb-focused)");

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
            last_arb_attempt: HashMap::new(),
            ledger,
            price_cache: Arc::new(RwLock::new(PriceCache::default())),
            _order_feed: None,
            _price_feed: None,
            _reconciliation_poller: reconciliation_poller,
            tick_size_cache: Arc::new(RwLock::new(HashMap::new())),
            neg_risk_cache: Arc::new(RwLock::new(HashMap::new())),
            arb_finder,
            pending_arb_orders: Arc::new(RwLock::new(HashMap::new())),
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

    /// Prefetch and cache tick sizes + neg_risk for all tokens - critical for fast arb execution
    /// This eliminates network calls during order building, saving ~20-50ms per order
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
        let neg_risk_cache = self.neg_risk_cache.clone();
        
        let results: Vec<_> = stream::iter(token_ids)
            .map(|token_id| {
                let client = client.clone();
                async move {
                    // Fetch tick size and neg_risk in parallel for this token
                    let tick_future = client.tick_size(&token_id);
                    let neg_risk_future = client.neg_risk(&token_id);
                    
                    let (tick_result, neg_risk_result) = tokio::join!(tick_future, neg_risk_future);
                    
                    let tick_size = tick_result.ok()
                        .map(|resp| resp.minimum_tick_size.as_decimal());
                    let neg_risk = neg_risk_result.ok()
                        .map(|resp| resp.neg_risk);
                    
                    (token_id, tick_size, neg_risk)
                }
            })
            .buffer_unordered(PREFETCH_CONCURRENCY)
            .collect()
            .await;
        
        // Cache values synchronously for fast lookup during order building
        let tick_count;
        let neg_risk_count;
        {
            let mut tick_guard = tick_cache.write();
            let mut neg_guard = neg_risk_cache.write();
            
            for (token_id, tick_size, neg_risk) in results {
                if let Some(ts) = tick_size {
                    debug!(token_id = %token_id, tick_size = %ts, "Cached tick size");
                    tick_guard.insert(token_id.clone(), ts);
                }
                if let Some(nr) = neg_risk {
                    debug!(token_id = %token_id, neg_risk = %nr, "Cached neg_risk");
                    neg_guard.insert(token_id, nr);
                }
            }
            
            tick_count = tick_guard.len();
            neg_risk_count = neg_guard.len();
        }
        
        info!(
            tick_sizes_cached = tick_count,
            neg_risk_cached = neg_risk_count,
            "Token metadata prefetched and cached"
        );
    }
    
    /// Get cached tick size (synchronous, no network call)
    fn get_cached_tick_size(&self, token_id: &str) -> Option<Decimal> {
        self.tick_size_cache.read().get(token_id).copied()
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

        // Start price feed with FULL DEPTH - passes state_store for arb scanning
        let state_store = self.arb_finder.state_store().clone();
        match spawn_price_feed(WS_SUB_URL, token_ids, self.price_cache.clone(), Some(state_store)) {
            Ok(handle) => {
                self._price_feed = Some(handle);
                info!("Price feed started (full depth mode)");
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

        // Update arb finder with latest prices from WebSocket cache
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
        
        // Clean up timed out arb orders (orders that didn't fill within timeout)
        self.cleanup_stale_arb_orders().await;

        // Scan for arb opportunities using cross-product of order book levels
        let predictions = self.arb_finder.scan();
        
        for prediction in predictions {
            // Only act on ExecuteNow for direct arb opportunities
            if prediction.recommended_action != RecommendedAction::ExecuteNow {
                continue;
            }
            
            // Check cooldown to avoid hammering the same market
            if !self.arb_cooldown_elapsed(&prediction.market_id) {
                trace!(market_id = %prediction.market_id, "Arb cooldown active");
                continue;
            }
            
            if let Some(market) = self.markets.get(&prediction.market_id).cloned() {
                // Get position state - include pending in exposure calc but DON'T block on them
                let snapshot = self.ledger.get_state(&prediction.market_id).await;
                
                // Include pending orders in exposure to avoid over-leveraging
                let effective_up = snapshot.position.up_shares + snapshot.pending_up;
                let effective_down = snapshot.position.down_shares + snapshot.pending_down;

                // Calculate safe order size based on effective position
                let (up_size, down_size) = self.arb_finder.calculate_safe_arb_size(
                    effective_up,
                    effective_down,
                    prediction.confidence,
                );

                // Get prices from prediction (these come from cross-product scan)
                let (up_price, down_price) = match (
                    prediction.up_target_price,
                    prediction.down_target_price,
                ) {
                    (Some(u), Some(d)) => (u, d),
                    _ => continue,
                };
                
                // Use the executable size from the prediction if available
                let exec_size = prediction.executable_size
                    .map(|s| s.min(up_size).min(down_size))
                    .unwrap_or_else(|| up_size.min(down_size));

                if exec_size <= Decimal::ZERO {
                    debug!(
                        market_id = %prediction.market_id,
                        effective_up = %effective_up,
                        effective_down = %effective_down,
                        "Skipping arb - max exposure reached"
                    );
                    continue;
                }

                let combined = up_price + down_price;
                let profit_per_share = Decimal::ONE - combined;
                
                // Use FOK only for confirmed arbs (confidence = 1.0) where we KNOW liquidity exists
                // Otherwise use GTC to let orders rest and catch fleeting opportunities
                let use_fok = prediction.confidence >= dec!(1.0);
                
                info!(
                    market_id = %prediction.market_id,
                    confidence = %prediction.confidence.round_dp(4),
                    up_price = %up_price,
                    down_price = %down_price,
                    combined = %combined,
                    profit_per_share = %profit_per_share,
                    size = %exec_size,
                    order_type = if use_fok { "FOK" } else { "GTC" },
                    "🚀 EXECUTING ARB"
                );

                let placed = self
                    .execute_arb_orders(&market, up_price, down_price, exec_size, exec_size, use_fok)
                    .await;

                // Record attempt time for cooldown
                self.last_arb_attempt.insert(market.market_id.clone(), Instant::now());

                info!(
                    market_id = %prediction.market_id,
                    placed,
                    "Arb execution complete"
                );
            }
        }
    }
    
    /// Check if cooldown has elapsed for a market (prevent hammering)
    fn arb_cooldown_elapsed(&self, market_id: &str) -> bool {
        self.last_arb_attempt
            .get(market_id)
            .map(|t| t.elapsed() >= Duration::from_millis(200)) // 200ms cooldown - be aggressive
            .unwrap_or(true)
    }
    
    /// Clean up arb orders that didn't fill within timeout
    async fn cleanup_stale_arb_orders(&self) {
        let timeout = Duration::from_secs(2); // Cancel unfilled arb orders after 2 seconds
        let mut to_cancel = Vec::new();
        
        {
            let orders = self.pending_arb_orders.read();
            for (order_id, pending) in orders.iter() {
                if pending.placed_at.elapsed() > timeout {
                    to_cancel.push(order_id.clone());
                }
            }
        }
        
        if !to_cancel.is_empty() {
            debug!(count = to_cancel.len(), "Cleaning up stale arb orders");
            
            // Remove from tracking FIRST - whether cancel succeeds or not, we're done tracking these
            {
                let mut orders = self.pending_arb_orders.write();
                for order_id in &to_cancel {
                    orders.remove(order_id);
                }
            }
            
            // Attempt cancel (best effort - orders may already be filled/cancelled)
            let _ = self.cancel_orders(&to_cancel).await;
        }
    }


    /// Execute arbitrage by submitting both Up and Down orders in parallel
    /// Uses cached tick sizes for speed
    /// `use_fok`: If true, use Fill-or-Kill orders - order fills completely or is cancelled
    ///            This is ideal for confirmed arbs where we know liquidity exists
    async fn execute_arb_orders(
        &self,
        market: &ActiveMarket,
        up_price: Decimal,
        down_price: Decimal,
        up_size: Decimal,
        down_size: Decimal,
        use_fok: bool,
    ) -> usize {
        let arb_start = Instant::now();

        // Build orders using CACHED tick sizes (no network calls!)
        let signed = self.build_arb_orders_fast(market, up_price, down_price, up_size, down_size, use_fok).await;
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
                let sides = [MarketSide::Up, MarketSide::Down];
                let sizes = [up_size, down_size];
                let prices = [up_price, down_price];
                
                for (i, resp) in responses.iter().enumerate() {
                    let success = resp
                        .error_msg
                        .as_ref()
                        .map(|s| s.is_empty())
                        .unwrap_or(true);

                    let side = sides.get(i).copied().unwrap_or(MarketSide::Up);
                    let size = sizes.get(i).copied().unwrap_or(Decimal::ZERO);
                    let price = prices.get(i).copied().unwrap_or(Decimal::ZERO);

                    if success {
                        let token_id = match side {
                            MarketSide::Up => &market.up_token_id,
                            MarketSide::Down => &market.down_token_id,
                        };

                        self.ledger
                            .record_order_placed(
                                resp.order_id.clone(),
                                market.market_id.clone(),
                                token_id.clone(),
                                side,
                                size,
                                price,
                            )
                            .await;
                        
                        // Track pending arb order for timeout cleanup
                        {
                            let mut pending = self.pending_arb_orders.write();
                            pending.insert(resp.order_id.clone(), PendingArbOrder {
                                order_id: resp.order_id.clone(),
                                market_id: market.market_id.clone(),
                                side,
                                placed_at: Instant::now(),
                            });
                        }

                        info!(
                            market_id = %market.market_id,
                            order_id = %resp.order_id,
                            side = ?side,
                            price = %price,
                            size = %size,
                            "Arb order placed"
                        );

                        placed += 1;
                    } else {
                        warn!(
                            market_id = %market.market_id,
                            order_id = %resp.order_id,
                            side = ?side,
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
    
    /// Build arb orders using cached tick sizes for maximum speed
    /// `use_fok`: If true, use Fill-or-Kill orders for immediate execution (confirmed arbs)
    async fn build_arb_orders_fast(
        &self,
        market: &ActiveMarket,
        up_price: Decimal,
        down_price: Decimal,
        up_size: Decimal,
        down_size: Decimal,
        use_fok: bool,
    ) -> Vec<SignedOrder> {
        let signer = self.signer.clone().with_chain_id(Some(POLYGON));
        let client = self.client.clone();
        
        // Get cached tick sizes (synchronous - no network call!)
        let up_tick = self.get_cached_tick_size(&market.up_token_id)
            .unwrap_or(dec!(0.01));
        let down_tick = self.get_cached_tick_size(&market.down_token_id)
            .unwrap_or(dec!(0.01));
        
        // Quantize prices to tick size
        let up_price_q = quantize_price(up_price, up_tick);
        let down_price_q = quantize_price(down_price, down_tick);
        
        // Build both orders in parallel
        let up_future = {
            let client = client.clone();
            let signer = signer.clone();
            let token_id = market.up_token_id.clone();
            async move {
                build_single_order(&client, &signer, &token_id, up_price_q, up_size, use_fok).await
            }
        };
        
        let down_future = {
            let client = client.clone();
            let signer = signer.clone();
            let token_id = market.down_token_id.clone();
            async move {
                build_single_order(&client, &signer, &token_id, down_price_q, down_size, use_fok).await
            }
        };
        
        let (up_result, down_result) = tokio::join!(up_future, down_future);
        
        let mut orders = Vec::with_capacity(2);
        if let Some(order) = up_result {
            orders.push(order);
        }
        if let Some(order) = down_result {
            orders.push(order);
        }
        
        orders
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
                // Only log if something was actually cancelled
                if !response.canceled.is_empty() {
                    info!(
                        cancelled = response.canceled.len(),
                        "Orders cancelled"
                    );
                }
                Ok(response.canceled)
            }
            Err(e) => {
                debug!(error = %e, "Cancel request failed");
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

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn markets(&self) -> &HashMap<String, ActiveMarket> {
        &self.markets
    }
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

/// Build a single signed order - used by build_arb_orders_fast
/// `use_fok`: If true, use Fill-or-Kill for immediate execution (confirmed arbs)
///            If false, use GTC (good-till-cancelled) for limit orders
async fn build_single_order(
    client: &AuthenticatedClient,
    signer: &alloy::signers::local::PrivateKeySigner,
    token_id: &str,
    price: Decimal,
    size: Decimal,
    use_fok: bool,
) -> Option<SignedOrder> {
    let (poly_price, poly_size) = convert_order_params(price, size)?;
    
    let order_type = if use_fok { OrderType::FOK } else { OrderType::GTC };
    
    trace!(
        token_id = %token_id,
        price = %poly_price,
        size = %poly_size,
        order_type = ?order_type,
        "Building order"
    );

    let signable = match client
        .limit_order()
        .token_id(token_id)
        .price(poly_price)
        .size(poly_size)
        .side(ClobSide::Buy)
        .order_type(order_type)
        .build()
        .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, token_id = %token_id, "Failed to build order");
            return None;
        }
    };

    match client.sign(signer, signable).await {
        Ok(s) => Some(s),
        Err(e) => {
            error!(error = %e, token_id = %token_id, "Failed to sign order");
            None
        }
    }
}
