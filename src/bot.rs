use crate::clob_api::fetch_token_balances;
use crate::config::Config;
use crate::ladder::{
    CostTracker, LadderConfig, LadderEngine, LadderOrder, LadderState, MarketPhase, ReladderReason,
};
use crate::market_cache::MarketCache;
use crate::models::MarketInfo;
use crate::models::{MarketEvent, now_ms};
use crate::poller::{
    LedgerHandle, MarketSide, spawn_ledger_actor, spawn_order_feed, spawn_reconciliation_poller,
};
use crate::price_feed::{PriceCache, RestPriceCache, spawn_price_feed, spawn_rest_price_poller};
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::{Timelike, Utc};
use futures_util::StreamExt;
use futures_util::stream;

use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{Side as ClobSide, SignatureType, SignedOrder};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal as PolyDecimal};
use polymarket_client_sdk::ws::config::Config as WsConfig;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;
type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(200);
const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";
const PREFETCH_CONCURRENCY: usize = 6;
const SIGN_CONCURRENCY: usize = 4;
const MIN_EVENT_INTERVAL: Duration = Duration::from_millis(50);
const REST_POLL_INTERVAL: Duration = Duration::from_millis(300);
const AUDIT_INTERVAL: Duration = Duration::from_secs(30);
const AUDIT_TOLERANCE: Decimal = dec!(0.5);

#[derive(Clone)]
pub struct ActiveMarket {
    pub market_id: String,
    pub asset: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub start_time: chrono::DateTime<Utc>,
    pub end_time: chrono::DateTime<Utc>,
}

impl ActiveMarket {
    fn from_info(info: &MarketInfo) -> Self {
        Self {
            market_id: info.id.clone(),
            asset: info.asset.clone(),
            up_token_id: info.up_token_id.clone(),
            down_token_id: info.down_token_id.clone(),
            start_time: info.start_time,
            end_time: info.end_time,
        }
    }
}

struct MarketAction {
    up_ask: Decimal,
    down_ask: Decimal,
    cancellations: Vec<String>,
    orders: Vec<LadderOrder>,
    total_up: Decimal,
    total_down: Decimal,
    reason: ReladderReason,
    phase: MarketPhase,
}

#[derive(Debug, Clone, Copy)]
enum PriceSource {
    Rest,
    Ws,
}

#[derive(Debug, Clone)]
struct PriceSnapshot {
    up: Decimal,
    down: Decimal,
    max_age_ms: u64,
}

pub struct SimpleBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    ws_client: Arc<AuthenticatedWsClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,

    markets: HashMap<String, ActiveMarket>,
    next_markets: HashMap<String, ActiveMarket>,
    tick_sizes: HashMap<String, Decimal>,

    /* Timing state */
    last_market_refresh: Instant,
    last_order_by_market: HashMap<String, Instant>,

    ledger: LedgerHandle,

    /* Price data from the websocket */
    price_cache: Arc<RwLock<PriceCache>>,
    /* Price data from the REST endpoint */
    rest_price_cache: Arc<RwLock<RestPriceCache>>,

    /* Background tasks */
    _order_feed: Option<tokio::task::JoinHandle<()>>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    _rest_poller: Option<tokio::task::JoinHandle<()>>,
    _reconciliation_poller: tokio::task::JoinHandle<()>,
    event_rx: Option<mpsc::Receiver<MarketEvent>>,

    /* Laddering strategy. */
    ladder_engine: LadderEngine,
    ladder_state: LadderState,
    cost_trackers: HashMap<String, CostTracker>,
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
            WsClient::new(WS_SUB_URL, WsConfig::default())?.authenticate(credentials, addr)?,
        );

        /* Spawn the inventory ledger. */
        let ledger = spawn_ledger_actor();

        /* HTTP reconciliation every 10 seconds. */
        let reconciliation_poller =
            spawn_reconciliation_poller(client.clone(), ledger.clone(), Duration::from_secs(10));

        let target_assets = config.target_assets.clone();

        let ladder_config = LadderConfig::default();

        Ok(Self {
            config,
            client,
            ws_client,
            signer,
            market_cache: MarketCache::new(target_assets),
            markets: HashMap::new(),
            next_markets: HashMap::new(),
            tick_sizes: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            ledger,
            price_cache: Arc::new(RwLock::new(PriceCache::default())),
            rest_price_cache: Arc::new(RwLock::new(RestPriceCache::default())),
            _order_feed: None,
            _price_feed: None,
            _rest_poller: None,
            _reconciliation_poller: reconciliation_poller,
            event_rx: None,
            ladder_engine: LadderEngine::new(ladder_config),
            ladder_state: LadderState::default(),

            cost_trackers: HashMap::new(),
        })
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
        let markets = all_markets
            .into_iter()
            .filter(|info| info.end_time > now)
            .map(|info| ActiveMarket::from_info(&info))
            .collect();

        Some(markets)
    }

    /* When the bot has begun, ensure that we have the full picture and capture any data from previous positions in current markets. */
    async fn init_positions(&self, markets: &[ActiveMarket]) {
        for market in markets {
            match fetch_token_balances(&self.client, &market.up_token_id, &market.down_token_id)
                .await
            {
                Ok((up_bal, down_bal)) => {
                    let up_cost = up_bal * self.config.target_total_cost;
                    let down_cost = down_bal * self.config.target_total_cost;
                    self.ledger
                        .set_initial_position(
                            market.market_id.clone(),
                            up_bal,
                            down_bal,
                            up_cost,
                            down_cost,
                        )
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

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");

        /* Fetch and filter markets. */
        let markets = match self.fetch_active_markets().await {
            Some(m) => m,
            None => return,
        };

        let now = Utc::now();
        let mut by_asset: HashMap<String, Vec<ActiveMarket>> = HashMap::new();
        for market in markets {
            by_asset
                .entry(market.asset.clone())
                .or_default()
                .push(market);
        }

        let current_by_asset: HashMap<String, ActiveMarket> = self
            .markets
            .values()
            .map(|m| (m.asset.clone(), m.clone()))
            .collect();

        let mut new_current = HashMap::new();
        let mut new_next = HashMap::new();
        let mut newly_active = Vec::new();

        for (asset, mut list) in by_asset {
            list.sort_by_key(|m| m.start_time);

            let existing = current_by_asset.get(&asset);
            let current = existing.filter(|m| m.end_time > now).cloned().or_else(|| {
                list.iter()
                    .find(|m| m.start_time <= now && m.end_time > now)
                    .cloned()
            });

            if let Some(current) = current {
                if existing
                    .map(|e| e.market_id != current.market_id)
                    .unwrap_or(true)
                {
                    newly_active.push(current.clone());
                }
                new_current.insert(current.market_id.clone(), current);
            }

            if let Some(next) = list
                .iter()
                .filter(|m| m.start_time > now)
                .min_by_key(|m| m.start_time)
                .cloned()
            {
                new_next.insert(next.market_id.clone(), next);
            }
        }

        for existing in current_by_asset.values() {
            if existing.end_time > now && !new_current.contains_key(&existing.market_id) {
                new_current.insert(existing.market_id.clone(), existing.clone());
            }
        }

        self.markets = new_current;
        self.next_markets = new_next;

        /* Log what we have found. */
        for market in self.markets.values() {
            info!(
                market_id = %market.market_id,
                asset = %market.asset,
                end_time = %market.end_time,
                "Market registered"
            );
        }
        for market in self.next_markets.values() {
            info!(
                market_id = %market.market_id,
                asset = %market.asset,
                start_time = %market.start_time,
                end_time = %market.end_time,
                "Market queued"
            );
        }

        /* Warm token metadata and start off feeds. */
        if !newly_active.is_empty() {
            let current = self.markets.values().cloned().collect::<Vec<_>>();
            let queued = self.next_markets.values().cloned().collect::<Vec<_>>();
            self.prefetch_token_metadata(&[current.clone(), queued].concat())
                .await;
            self.init_positions(&newly_active).await;
            self.start_feeds(&current);
        }
    }

    /* For all tokens that we are currently trading, fetch the metadata (tick size, fee rate, etc)
     * so that we don't make separate calls when placing the order.
     */
    async fn prefetch_token_metadata(&mut self, markets: &[ActiveMarket]) {
        let mut token_ids = Vec::new();
        for market in markets {
            token_ids.push(market.up_token_id.clone());
            token_ids.push(market.down_token_id.clone());
        }
        token_ids.sort();
        token_ids.dedup();

        let client = self.client.clone();
        let results = stream::iter(token_ids)
            .map(|token_id| {
                let client = client.clone();
                async move {
                    let tick_size = client
                        .tick_size(&token_id)
                        .await
                        .ok()
                        .map(|resp| resp.minimum_tick_size.as_decimal());
                    let _ = client.neg_risk(&token_id).await;
                    (token_id, tick_size)
                }
            })
            .buffer_unordered(PREFETCH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        for (token_id, tick_size) in results {
            if let Some(size) = tick_size {
                self.tick_sizes.insert(token_id, size);
            }
        }
    }

    fn start_feeds(&mut self, markets: &[ActiveMarket]) {
        if let Some(h) = self._price_feed.take() {
            h.abort();
        }
        if let Some(h) = self._order_feed.take() {
            h.abort();
        }
        if let Some(h) = self._rest_poller.take() {
            h.abort();
        }
        self.event_rx = None;

        // Collect IDs
        let market_ids: Vec<String> = markets.iter().map(|m| m.market_id.clone()).collect();
        let token_ids: Vec<String> = markets
            .iter()
            .flat_map(|m| [m.up_token_id.clone(), m.down_token_id.clone()])
            .collect();

        let (event_tx, event_rx) = mpsc::channel(256);
        self.event_rx = Some(event_rx);

        // Start price feed
        match spawn_price_feed(
            WS_SUB_URL,
            token_ids.clone(),
            self.price_cache.clone(),
            event_tx.clone(),
        ) {
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
            event_tx.clone(),
        ));
        info!("Order feed started");

        self._rest_poller = Some(spawn_rest_price_poller(
            self.client.clone(),
            token_ids,
            self.rest_price_cache.clone(),
            event_tx,
            REST_POLL_INTERVAL,
        ));
        info!("REST price poller started");
    }

    fn roll_over_markets(&mut self) -> Vec<ActiveMarket> {
        let now = Utc::now();
        let mut newly_active = Vec::new();
        let mut to_remove = Vec::new();
        let mut promoted = Vec::new();

        for market in self.markets.values() {
            if market.end_time <= now {
                if let Some(next) = self
                    .next_markets
                    .values()
                    .filter(|m| m.asset == market.asset && m.start_time <= now)
                    .min_by_key(|m| m.start_time)
                    .cloned()
                {
                    promoted.push(next);
                }
                to_remove.push(market.market_id.clone());
            }
        }

        for market_id in to_remove {
            self.markets.remove(&market_id);
            self.cost_trackers.remove(&market_id);
            self.ladder_state.clear_market(&market_id);
        }

        for next in promoted {
            self.next_markets.remove(&next.market_id);
            self.markets.insert(next.market_id.clone(), next.clone());
            newly_active.push(next);
        }

        newly_active
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
                if !response.not_canceled.is_empty() {
                    let not_cancelled_ids: Vec<String> =
                        response.not_canceled.keys().cloned().collect();
                    self.reconcile_not_cancelled(&not_cancelled_ids).await;
                }
                Ok(response.canceled)
            }
            Err(e) => {
                error!(error = %e, "Batch cancel failed");
                Err(e.into())
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
        let tick_up = self.tick_sizes.get(&up_token_id).copied();
        let tick_down = self.tick_sizes.get(&down_token_id).copied();

        stream::iter(orders.iter().cloned())
            .map(|order| {
                let client = client.clone();
                let signer = signer.clone();
                let up_token_id = up_token_id.clone();
                let down_token_id = down_token_id.clone();
                let tick_up = tick_up;
                let tick_down = tick_down;
                async move {
                    let token_id = match order.side {
                        MarketSide::Up => up_token_id.as_str(),
                        MarketSide::Down => down_token_id.as_str(),
                    };

                    let cached_tick = match order.side {
                        MarketSide::Up => tick_up,
                        MarketSide::Down => tick_down,
                    };
                    let tick_size = if let Some(size) = cached_tick {
                        size
                    } else {
                        match client.tick_size(token_id).await {
                            Ok(resp) => resp.minimum_tick_size.as_decimal(),
                            Err(e) => {
                                error!(token_id = %token_id, error = %e, "Missing tick size");
                                return None;
                            }
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

    async fn reconcile_not_cancelled(&self, order_ids: &[String]) {
        for order_id in order_ids {
            match self.client.order(order_id).await {
                Ok(order) => {
                    self.ledger
                        .apply_cancel_status(order_id.clone(), order.size_matched, order.status)
                        .await;
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("404 Not Found")
                        || msg.contains("Unable to find requested resource")
                    {
                        self.ledger
                            .apply_order_status(order_id.clone(), None, Some(false))
                            .await;
                        return;
                    }
                    warn!(
                        order_id = %order_id,
                        error = %e,
                        "Failed to fetch order after cancel"
                    );
                }
            }
        }
    }

    async fn audit_positions(&self) {
        let markets: Vec<ActiveMarket> = self.markets.values().cloned().collect();
        for market in markets {
            let snapshot = self.ledger.get_state(&market.market_id).await;
            let clob_actual =
                fetch_token_balances(&self.client, &market.up_token_id, &market.down_token_id)
                    .await;
            let Ok((up_clob, down_clob)) = clob_actual else {
                continue;
            };
            let up_delta = up_clob - snapshot.position.up_shares;
            let down_delta = down_clob - snapshot.position.down_shares;

            if up_delta.abs() >= AUDIT_TOLERANCE || down_delta.abs() >= AUDIT_TOLERANCE {
                warn!(
                    market_id = %market.market_id,
                    up_ledger = %snapshot.position.up_shares,
                    up_actual = %up_clob,
                    up_delta = %up_delta,
                    down_ledger = %snapshot.position.down_shares,
                    down_actual = %down_clob,
                    down_delta = %down_delta,
                    "Inventory audit mismatch"
                );
            }
        }
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;
        info!("Entering main loop...");

        let mut refresh_interval = tokio::time::interval(Duration::from_secs(1));
        let mut audit_interval = tokio::time::interval(AUDIT_INTERVAL);
        let mut scan_interval = tokio::time::interval(MIN_EVENT_INTERVAL);
        let mut last_scan = Instant::now() - MIN_EVENT_INTERVAL;
        let mut last_audit = Instant::now() - AUDIT_INTERVAL;

        loop {
            if let Some(rx) = &mut self.event_rx {
                tokio::select! {
                    _ = refresh_interval.tick() => {
                        let newly_active = self.roll_over_markets();
                        if !newly_active.is_empty() {
                            let current = self.markets.values().cloned().collect::<Vec<_>>();
                            let queued = self.next_markets.values().cloned().collect::<Vec<_>>();
                            self.prefetch_token_metadata(&[current.clone(), queued].concat())
                                .await;
                            self.init_positions(&newly_active).await;
                            self.start_feeds(&current);
                        }

                        if self.should_refresh_markets() {
                            self.discover_markets().await;
                            self.last_market_refresh = Instant::now();
                        }
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(MarketEvent::Price(_)) => {
                                if last_scan.elapsed() >= MIN_EVENT_INTERVAL {
                                    self.scan().await;
                                    last_scan = Instant::now();
                                }
                            }
                            // V2 CHANGE: React to fills immediately!
                            Some(MarketEvent::Order(market_id)) => {
                                debug!(market_id = %market_id, "Fill event received - evaluating immediately");
                                if let Some(market) = self.markets.get(&market_id).cloned() {
                                    if let Some(action) = self.evaluate_market(&market).await {
                                        self.execute_action(&market, action).await;
                                    }
                                }
                            }
                            None => {}
                        }
                    }
                    _ = scan_interval.tick() => {
                        if last_scan.elapsed() >= MIN_EVENT_INTERVAL {
                            self.scan().await;
                            last_scan = Instant::now();
                        }
                    }
                    _ = audit_interval.tick() => {
                        self.audit_positions().await;
                    }
                }
            } else {
                // Fallback polling mode (no event channel)
                let newly_active = self.roll_over_markets();
                if !newly_active.is_empty() {
                    let current = self.markets.values().cloned().collect::<Vec<_>>();
                    let queued = self.next_markets.values().cloned().collect::<Vec<_>>();
                    self.prefetch_token_metadata(&[current.clone(), queued].concat())
                        .await;
                    self.init_positions(&newly_active).await;
                    self.start_feeds(&current);
                }

                if self.should_refresh_markets() {
                    self.discover_markets().await;
                    self.last_market_refresh = Instant::now();
                }

                self.scan().await;
                if last_audit.elapsed() >= AUDIT_INTERVAL {
                    self.audit_positions().await;
                    last_audit = Instant::now();
                }
                tokio::time::sleep(LOOP_DELAY).await;
            }
        }
    }

    async fn scan(&mut self) {
        let markets: Vec<ActiveMarket> = self.markets.values().cloned().collect();

        for market in markets {
            if let Some(action) = self.evaluate_market(&market).await {
                self.execute_action(&market, action).await;
            }
        }
    }

    async fn evaluate_market(&mut self, market: &ActiveMarket) -> Option<MarketAction> {
        // Get prices
        let now = now_ms() as u64;
        let (rest_snapshot, ws_snapshot) =
            self.read_price_sources(&market.up_token_id, &market.down_token_id, now);
        let (up_ask, down_ask, source) =
            self.select_price_source(rest_snapshot.as_ref(), ws_snapshot.as_ref())?;

        trace!(
            market_id = %market.market_id,
            up_ask = %up_ask,
            down_ask = %down_ask,
            source = ?source,
            "Price check"
        );

        // Get ledger state
        let snapshot = self.ledger.get_state(&market.market_id).await;

        // Calculate market phase based on time remaining
        let seconds_remaining = (market.end_time - Utc::now()).num_seconds();
        let phase = MarketPhase::from_seconds_remaining(
            seconds_remaining,
            self.ladder_engine.config().late_game_seconds,
        );

        // Check if we should reladder
        let reason = self.ladder_state.should_reladder(
            &market.market_id,
            up_ask,
            down_ask,
            &snapshot.position,
            self.ladder_engine.config(),
            now,
        );

        let reason = match reason {
            Some(r) => {
                debug!(
                    market_id = %market.market_id,
                    reason = ?r,
                    phase = ?phase,
                    imbalance = %(snapshot.position.up_shares - snapshot.position.down_shares),
                    seconds_remaining = seconds_remaining,
                    "Reladder triggered"
                );
                r
            }
            None => {
                if !self.cooldown_elapsed(&market.market_id) {
                    return None;
                }
                // No reason to reladder, skip
                return None;
            }
        };

        // Get or create cost tracker for this market
        let cost_tracker = self
            .cost_trackers
            .entry(market.market_id.clone())
            .or_insert_with(CostTracker::new);

        // Compute ladder plan
        let mut plan = self.ladder_engine.compute_ladder(
            up_ask,
            down_ask,
            &snapshot.position,
            snapshot.pending_up,
            snapshot.pending_down,
            &snapshot.open_orders,
            cost_tracker,
            phase,
            reason,
        );

        // Add stale cancellations (the ONLY place we cancel)
        let stale_cancels = self.ladder_state.stale_cancellations(
            &snapshot.open_orders,
            up_ask,
            down_ask,
            self.ladder_engine.config(),
        );
        for order_id in stale_cancels {
            if !plan.cancellations.contains(&order_id) {
                plan.cancellations.push(order_id);
            }
        }

        if plan.cancellations.is_empty() && plan.orders.is_empty() {
            return None;
        }

        let total_up = snapshot.position.up_shares + snapshot.pending_up;
        let total_down = snapshot.position.down_shares + snapshot.pending_down;

        Some(MarketAction {
            up_ask,
            down_ask,
            cancellations: plan.cancellations,
            orders: plan.orders,
            total_up,
            total_down,
            reason,
            phase,
        })
    }

    async fn execute_action(&mut self, market: &ActiveMarket, action: MarketAction) {
        let market_id = &market.market_id;
        let mut cancelled_count = 0;

        // Execute cancellations
        if !action.cancellations.is_empty() {
            match self.cancel_orders(&action.cancellations).await {
                Ok(cancelled) => {
                    cancelled_count = cancelled.len();
                    info!(
                        market_id,
                        count = cancelled.len(),
                        reason = ?action.reason,
                        "Cancelled stale orders"
                    );
                    self.ledger.mark_orders_cancelled(cancelled).await;
                }
                Err(e) => {
                    error!(market_id, error = %e, "Failed to cancel orders");
                }
            }
        }

        // Place new orders
        if !action.orders.is_empty() {
            let aggressive_count = action.orders.iter().filter(|o| o.is_aggressive).count();
            let recovery_count = action.orders.iter().filter(|o| o.is_recovery_hunt).count();
            let passive_count = action.orders.len() - aggressive_count - recovery_count;

            if aggressive_count > 0 || recovery_count > 0 {
                info!(
                    market_id,
                    aggressive = aggressive_count,
                    recovery = recovery_count,
                    passive = passive_count,
                    phase = ?action.phase,
                    "Placing orders"
                );
            }

            let placed_orders = self.place_orders(market, &action.orders).await;

            if !placed_orders.is_empty() {
                let placed_up: Decimal = placed_orders
                    .iter()
                    .filter(|o| o.side == MarketSide::Up)
                    .map(|o| o.size)
                    .sum();
                let placed_down: Decimal = placed_orders
                    .iter()
                    .filter(|o| o.side == MarketSide::Down)
                    .map(|o| o.size)
                    .sum();

                // Log average cost if we have completed pairs
                let avg_cost_str = self
                    .cost_trackers
                    .get(market_id)
                    .and_then(|ct| ct.avg_pair_cost())
                    .map(|avg| format!("{:.4}", avg))
                    .unwrap_or_else(|| "n/a".to_string());

                info!(
                    market_id,
                    up_ask = %action.up_ask,
                    down_ask = %action.down_ask,
                    cancelled = cancelled_count,
                    placed = placed_orders.len(),
                    placed_up = %placed_up,
                    placed_down = %placed_down,
                    avg_pair_cost = %avg_cost_str,
                    phase = ?action.phase,
                    reason = ?action.reason,
                    "Ladder updated"
                );

                // Record ladder state
                let snapshot = self.ledger.get_state(market_id).await;
                self.ladder_state.record_ladder(
                    market_id.clone(),
                    action.up_ask,
                    action.down_ask,
                    &snapshot.position,
                    now_ms() as u64,
                );
            }
        }

        self.last_order_by_market
            .insert(market_id.clone(), Instant::now());
    }

    async fn place_orders(
        &self,
        market: &ActiveMarket,
        orders: &[LadderOrder],
    ) -> Vec<LadderOrder> {
        if should_reject_pair(orders) {
            warn!(
                market_id = %market.market_id,
                "Order pair rejected: notional below $1"
            );
            return Vec::new();
        }

        let now = now_ms() as u64;
        let (rest_snapshot, ws_snapshot) =
            self.read_price_sources(&market.up_token_id, &market.down_token_id, now);
        let (_up_ask, _down_ask, _) =
            match self.select_price_source(rest_snapshot.as_ref(), ws_snapshot.as_ref()) {
                Some(prices) => prices,
                None => return Vec::new(),
            };

        let build_start = Instant::now();
        let signed = self.build_signed_orders(orders, market).await;
        let build_ms = build_start.elapsed().as_millis();

        if signed.is_empty() {
            return Vec::new();
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
                let mut placed = Vec::new();

                for (resp, order) in responses.iter().zip(orders.iter()) {
                    let success = resp
                        .error_msg
                        .as_ref()
                        .map(|s| s.is_empty())
                        .unwrap_or(true);

                    if success {
                        // V2 CHANGE: Log aggressive orders
                        if order.is_aggressive {
                            info!(
                                order_id = %resp.order_id,
                                side = ?order.side,
                                price = %order.price,
                                size = %order.size,
                                "Aggressive rebalance order placed"
                            );
                        }

                        self.ledger
                            .record_order_placed(
                                resp.order_id.clone(),
                                market.market_id.clone(),
                                order.side,
                                order.size,
                                order.price,
                            )
                            .await;

                        placed.push(order.clone());
                    } else {
                        warn!(
                            market_id = %market.market_id,
                            order_id = %resp.order_id,
                            error = ?resp.error_msg,
                            is_aggressive = order.is_aggressive,
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
                Vec::new()
            }
        }
    }

    fn read_price_sources(
        &self,
        up_token_id: &str,
        down_token_id: &str,
        now_ms: u64,
    ) -> (Option<PriceSnapshot>, Option<PriceSnapshot>) {
        let rest = {
            let cache = self.rest_price_cache.read();
            let (_, up, up_age) = match cache.get_with_age(up_token_id, now_ms) {
                Some(v) => v,
                None => {
                    return (
                        None,
                        self.read_ws_snapshot(up_token_id, down_token_id, now_ms),
                    );
                }
            };
            let (_, down, down_age) = match cache.get_with_age(down_token_id, now_ms) {
                Some(v) => v,
                None => {
                    return (
                        None,
                        self.read_ws_snapshot(up_token_id, down_token_id, now_ms),
                    );
                }
            };
            Some(PriceSnapshot {
                up,
                down,
                max_age_ms: up_age.max(down_age),
            })
        };

        let ws = self.read_ws_snapshot(up_token_id, down_token_id, now_ms);
        (rest, ws)
    }

    fn read_ws_snapshot(
        &self,
        up_token_id: &str,
        down_token_id: &str,
        now_ms: u64,
    ) -> Option<PriceSnapshot> {
        let cache = self.price_cache.read();
        let (_, up, up_age) = cache.get_with_age(up_token_id, now_ms)?;
        let (_, down, down_age) = cache.get_with_age(down_token_id, now_ms)?;
        Some(PriceSnapshot {
            up,
            down,
            max_age_ms: up_age.max(down_age),
        })
    }

    fn select_price_source(
        &self,
        rest: Option<&PriceSnapshot>,
        ws: Option<&PriceSnapshot>,
    ) -> Option<(Decimal, Decimal, PriceSource)> {
        let max_age = self.config.max_price_age_ms as u64;
        if let Some(snapshot) = rest.filter(|s| s.max_age_ms <= max_age) {
            return Some((snapshot.up, snapshot.down, PriceSource::Rest));
        }
        ws.filter(|s| s.max_age_ms <= max_age)
            .map(|snapshot| (snapshot.up, snapshot.down, PriceSource::Ws))
    }

    fn cooldown_elapsed(&self, market_id: &str) -> bool {
        self.last_order_by_market
            .get(market_id)
            .map(|t| t.elapsed() >= Duration::from_secs(self.config.cooldown_secs))
            .unwrap_or(true)
    }

    fn should_refresh_markets(&self) -> bool {
        if self.last_market_refresh.elapsed() < Duration::from_secs(30) {
            return false;
        }

        let now = Utc::now();
        let total_seconds = now.minute() * 60 + now.second();
        let position_in_window = total_seconds % 900;

        (840..870).contains(&position_in_window)
    }
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn markets(&self) -> &HashMap<String, ActiveMarket> {
        &self.markets
    }
}

fn should_reject_pair(orders: &[LadderOrder]) -> bool {
    /* checks if a given order pair has any orders that will be less than a dollar in value. */
    let mut up_min: Option<Decimal> = None;
    let mut down_min: Option<Decimal> = None;

    for order in orders {
        let notional = order.price * order.size;
        match order.side {
            MarketSide::Up => {
                up_min = Some(up_min.map_or(notional, |v| v.min(notional)));
            }
            MarketSide::Down => {
                down_min = Some(down_min.map_or(notional, |v| v.min(notional)));
            }
        }
    }

    match (up_min, down_min) {
        (Some(up), Some(down)) => up < dec!(1.00) || down < dec!(1.00),
        _ => false,
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
