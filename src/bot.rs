use crate::chainlink::ChainlinkClient;
use crate::config::Config;
use crate::edge::{ArbDetector, BuyReason, BuySignal, MarketSnapshot, Side};
use crate::market_cache::MarketCache;
use crate::models::{MarketState, TradingPair};

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

// Configuration constants
const CHAINLINK_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const STATUS_PRINT_INTERVAL: Duration = Duration::from_secs(5);
const SCAN_LOOP_DELAY: Duration = Duration::from_millis(100);
const OPEN_PRICE_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const OPEN_PRICE_RETRY_DELAY: Duration = Duration::from_millis(500);
const OPEN_PRICE_MAX_RETRIES: u32 = 3;
const DEFAULT_WINDOW_DURATION: Duration = Duration::from_secs(900);

// Order pricing constants
const LIMIT_ORDER_BUFFER: Decimal = dec!(0.005);
const EMERGENCY_TAKER_BUFFER: Decimal = dec!(0.01);
const TICK_SIZE_DP: u32 = 2; // Polymarket uses 0.01 tick size (2 decimal places)
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const MIN_EDGE_AFTER_BUFFER: Decimal = dec!(0.01); // Minimum profit margin after tick rounding

/// Round price down to valid tick size for buy orders (conservative)
fn round_to_tick_buy(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::ToZero)
}

/// Round price up to valid tick size for aggressive buys (ensure fill)
fn round_to_tick_up(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::AwayFromZero)
}

/// Calculate limit price for a buy order, ensuring it's within valid bounds
/// Returns None if no valid price exists (signal price too low)
fn calculate_buy_limit(signal_price: Decimal, buffer: Decimal, aggressive: bool) -> Option<Decimal> {
    let raw_price = if aggressive {
        signal_price + buffer
    } else {
        signal_price - buffer
    };
    
    let rounded = if aggressive {
        round_to_tick_up(raw_price)
    } else {
        round_to_tick_buy(raw_price)
    };
    
    // Clamp to valid range
    let clamped = rounded.max(MIN_PRICE).min(MAX_PRICE);
    
    // If we had to raise the price to MIN_PRICE and it's now >= signal, it's not a good trade
    if !aggressive && clamped >= signal_price {
        return None;
    }
    
    Some(clamped)
}

#[derive(Debug, Clone)]
pub struct ArbPosition {
    pub market_id: String,
    pub asset: String,
    pub first_side: Side,
    pub first_price: Decimal,
    pub first_order_id: Option<String>,
    pub second_side: Option<Side>,
    pub second_price: Option<Decimal>,
    pub second_order_id: Option<String>,
    pub size: Decimal,
    pub created_at: DateTime<Utc>,
}

impl ArbPosition {
    pub fn is_complete(&self) -> bool {
        self.second_side.is_some()
    }

    pub fn total_cost(&self) -> Decimal {
        self.first_price + self.second_price.unwrap_or(dec!(0))
    }

    pub fn profit(&self) -> Option<Decimal> {
        self.second_price.map(|_| dec!(1) - self.total_cost())
    }

    pub fn age_seconds(&self, now: DateTime<Utc>) -> i64 {
        (now - self.created_at).num_seconds()
    }
}

pub struct HighFreqArbBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    chainlink: ChainlinkClient,
    arb_detector: ArbDetector,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    positions: HashMap<String, ArbPosition>,
    current_window_end: Option<DateTime<Utc>>,
    last_status_print: Instant,
    last_chainlink_update: Instant,
    chainlink_prices: HashMap<String, Decimal>,
}

impl HighFreqArbBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing HighFreqArbBot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        debug!("Market cache initialized");

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| {
                error!(error = %e, "Failed to parse private key");
                e
            })?;
        debug!("Signer created successfully");

        info!("Authenticating with Polymarket CLOB");
        let client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())
            .map_err(|e| {
                error!(error = %e, "Failed to create CLOB client");
                e
            })?
            .authentication_builder(&signer.clone().with_chain_id(Some(POLYGON)))
            .authenticate()
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to authenticate with CLOB");
                e
            })?;
        info!("Successfully authenticated with Polymarket CLOB");

        Ok(Self {
            config,
            client: Arc::new(client),
            signer,
            market_cache,
            chainlink: ChainlinkClient::new(),
            arb_detector: ArbDetector::default(),
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            positions: HashMap::new(),
            current_window_end: None,
            last_status_print: Instant::now(),
            last_chainlink_update: Instant::now(),
            chainlink_prices: HashMap::new(),
        })
    }

    pub async fn run(&mut self) {
        info!(
            dry_run = self.config.dry_run,
            shares_per_side = %self.config.arb_config.shares_per_side,
            "Bot starting - single market per asset mode"
        );

        self.discover_markets().await;

        info!("Entering main loop");
        let mut loop_count: u64 = 0;

        loop {
            loop_count += 1;
            let now = Utc::now();

            if self.should_rotate_markets(now) {
                info!(
                    positions_cleared = self.positions.len(),
                    "Window expired, rotating markets"
                );
                self.positions.clear();
                self.discover_markets().await;
                continue;
            }

            if self.last_chainlink_update.elapsed() > CHAINLINK_UPDATE_INTERVAL {
                self.update_prices().await;
                self.last_chainlink_update = Instant::now();
            }

            if self.last_status_print.elapsed() > STATUS_PRINT_INTERVAL {
                self.log_status();
                self.last_status_print = Instant::now();
            }

            self.scan().await;

            if loop_count % 1000 == 0 {
                debug!(
                    loop_count,
                    markets = self.markets.len(),
                    positions = self.positions.len(),
                    "Main loop heartbeat"
                );
            }

            tokio::time::sleep(SCAN_LOOP_DELAY).await;
        }
    }

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    async fn scan(&mut self) {
        let mut first_leg_signals = Vec::new();
        let mut second_leg_signals = Vec::new();

        for (market_id, state) in &self.markets {
            let snap = match self.build_snapshot(market_id, state) {
                Some(s) => s,
                None => continue,
            };

            if let Some(position) = self.positions.get(market_id) {
                if !position.is_complete() {
                    if let Some(signal) = self.arb_detector.check_second_leg(
                        &snap,
                        position.first_side,
                        position.first_price,
                    ) {
                        // Pre-validate: can we get a valid limit price?
                        let is_emergency = signal.reason == BuyReason::EmergencyMop;
                        if let Some(limit_price) = calculate_buy_limit(
                            signal.price,
                            if is_emergency { EMERGENCY_TAKER_BUFFER } else { LIMIT_ORDER_BUFFER },
                            is_emergency,
                        ) {
                            // Check combined cost still profitable
                            let combined_cost = position.first_price + limit_price;
                            let edge = dec!(1) - combined_cost;
                            
                            if edge >= MIN_EDGE_AFTER_BUFFER || is_emergency {
                                debug!(
                                    market_id,
                                    side = ?signal.side,
                                    signal_price = %signal.price,
                                    limit_price = %limit_price,
                                    combined_cost = %combined_cost,
                                    edge = %edge,
                                    reason = ?signal.reason,
                                    "Second leg signal validated"
                                );
                                second_leg_signals.push((market_id.clone(), signal));
                            } else {
                                debug!(
                                    market_id,
                                    edge = %edge,
                                    min_required = %MIN_EDGE_AFTER_BUFFER,
                                    "Second leg signal rejected: insufficient edge after rounding"
                                );
                            }
                        } else {
                            debug!(
                                market_id,
                                signal_price = %signal.price,
                                "Second leg signal rejected: no valid limit price"
                            );
                        }
                    }
                }
            } else if let Some(signal) = self.arb_detector.check_first_leg(&snap) {
                // Pre-validate: can we get a valid limit price?
                if let Some(limit_price) = calculate_buy_limit(signal.price, LIMIT_ORDER_BUFFER, false) {
                    // Check if there's potential for second leg
                    let opposite_ask = if signal.side == Side::Up {
                        snap.pm_down_ask
                    } else {
                        snap.pm_up_ask
                    };
                    
                    let potential_combined = limit_price + opposite_ask;
                    let potential_edge = dec!(1) - potential_combined;
                    
                    if potential_edge >= MIN_EDGE_AFTER_BUFFER {
                        debug!(
                            market_id,
                            asset = %signal.asset,
                            side = ?signal.side,
                            signal_price = %signal.price,
                            limit_price = %limit_price,
                            opposite_ask = %opposite_ask,
                            potential_edge = %potential_edge,
                            reason = ?signal.reason,
                            "First leg signal validated"
                        );
                        first_leg_signals.push(signal);
                    } else {
                        debug!(
                            market_id,
                            potential_edge = %potential_edge,
                            min_required = %MIN_EDGE_AFTER_BUFFER,
                            "First leg signal rejected: insufficient potential edge"
                        );
                    }
                } else {
                    debug!(
                        market_id,
                        signal_price = %signal.price,
                        "First leg signal rejected: no valid limit price"
                    );
                }
            }
        }

        let total_signals = first_leg_signals.len() + second_leg_signals.len();
        if total_signals > 0 {
            info!(
                first_leg = first_leg_signals.len(),
                second_leg = second_leg_signals.len(),
                "Processing validated signals"
            );
        }

        // Execute second legs first (close positions before opening new ones)
        for (market_id, signal) in second_leg_signals {
            self.execute_second_leg(&market_id, &signal).await;
        }

        // Then execute first legs
        for signal in first_leg_signals {
            self.execute_first_leg(&signal).await;
        }
    }

    #[instrument(skip(self), fields(
        market_id = %signal.market_id,
        asset = %signal.asset,
        side = ?signal.side,
        price = %signal.price
    ))]
    async fn execute_first_leg(&mut self, signal: &BuySignal) {
        let state = match self.markets.get(&signal.market_id) {
            Some(s) => s,
            None => {
                error!(market_id = %signal.market_id, "Market not found for first leg");
                return;
            }
        };

        let token_id = match signal.side {
            Side::Up => &state.info.up_token_id,
            Side::Down => &state.info.down_token_id,
        };

        // Calculate valid limit price
        let limit_price = match calculate_buy_limit(signal.price, LIMIT_ORDER_BUFFER, false) {
            Some(price) => price,
            None => {
                warn!(
                    signal_price = %signal.price,
                    min_price = %MIN_PRICE,
                    "Signal price too low - no valid limit price possible"
                );
                return;
            }
        };

        info!(
            token_id,
            signal_price = %signal.price,
            limit_price = %limit_price,
            size = %self.config.arb_config.shares_per_side,
            dry_run = self.config.dry_run,
            "Executing first leg order"
        );

        let order_id = if self.config.dry_run {
            debug!("Dry run mode - simulating order placement");
            Some(format!("dry-run-{}", Utc::now().timestamp_millis()))
        } else {
            match self.place_order(token_id, limit_price).await {
                Ok(oid) => {
                    info!(order_id = %oid, "First leg order placed successfully");
                    Some(oid)
                }
                Err(e) => {
                    error!(error = %e, "Failed to place first leg order - NOT opening position");
                    return; // Don't create position if order failed
                }
            }
        };

        let position = ArbPosition {
            market_id: signal.market_id.clone(),
            asset: signal.asset.clone(),
            first_side: signal.side,
            first_price: limit_price,
            first_order_id: order_id,
            second_side: None,
            second_price: None,
            second_order_id: None,
            size: self.config.arb_config.shares_per_side,
            created_at: Utc::now(),
        };

        info!(
            market_id = %position.market_id,
            asset = %position.asset,
            side = ?position.first_side,
            price = %position.first_price,
            "Position opened"
        );

        self.positions.insert(signal.market_id.clone(), position);
    }

    #[instrument(skip(self), fields(
        market_id = %market_id,
        side = ?signal.side,
        price = %signal.price,
        reason = ?signal.reason
    ))]
    async fn execute_second_leg(&mut self, market_id: &str, signal: &BuySignal) {
        let (token_id, position_size, first_price) = {
            let state = match self.markets.get(market_id) {
                Some(s) => s,
                None => {
                    error!(market_id, "Market not found for second leg");
                    return;
                }
            };

            let position = match self.positions.get(market_id) {
                Some(p) => p,
                None => {
                    error!(market_id, "Position not found for second leg");
                    return;
                }
            };

            let tid = match signal.side {
                Side::Up => state.info.up_token_id.clone(),
                Side::Down => state.info.down_token_id.clone(),
            };

            (tid, position.size, position.first_price)
        };

        let is_emergency = signal.reason == BuyReason::EmergencyMop;
        
        let limit_price = if is_emergency {
            warn!(market_id, "Emergency mop - using aggressive taker price");
            // For emergency, we MUST fill, so be aggressive
            match calculate_buy_limit(signal.price, EMERGENCY_TAKER_BUFFER, true) {
                Some(price) => price,
                None => {
                    error!("Cannot calculate emergency price");
                    return;
                }
            }
        } else {
            match calculate_buy_limit(signal.price, LIMIT_ORDER_BUFFER, false) {
                Some(price) => price,
                None => {
                    warn!(
                        signal_price = %signal.price,
                        "Signal price too low for second leg - skipping"
                    );
                    return;
                }
            }
        };

        info!(
            token_id = %token_id,
            signal_price = %signal.price,
            limit_price = %limit_price,
            size = %position_size,
            is_emergency,
            dry_run = self.config.dry_run,
            "Executing second leg order"
        );

        let order_id = if self.config.dry_run {
            debug!("Dry run mode - simulating order placement");
            Some(format!("dry-run-{}", Utc::now().timestamp_millis()))
        } else {
            match self.place_order(&token_id, limit_price).await {
                Ok(oid) => {
                    info!(order_id = %oid, "Second leg order placed successfully");
                    Some(oid)
                }
                Err(e) => {
                    error!(error = %e, "Failed to place second leg order - position remains open");
                    return; // Don't mark complete if order failed
                }
            }
        };

        if let Some(position) = self.positions.get_mut(market_id) {
            position.second_side = Some(signal.side);
            position.second_price = Some(limit_price);
            position.second_order_id = order_id;

            let total_cost = position.total_cost();
            let profit = position.profit().unwrap_or(dec!(0));
            
            info!(
                market_id,
                first_price = %first_price,
                second_price = %limit_price,
                total_cost = %total_cost,
                profit = %profit,
                profit_pct = %(profit / total_cost * dec!(100)),
                "Position completed successfully"
            );
        }
    }

    #[instrument(skip(self), fields(token_id = %token_id, price = %price))]
    async fn place_order(&self, token_id: &str, price: Decimal) -> anyhow::Result<String> {
        let poly_price = PolyDecimal::try_from(price.to_string().as_str())
            .map_err(|e| anyhow::anyhow!("Invalid price format: {}", e))?;

        let poly_size =
            PolyDecimal::try_from(self.config.arb_config.shares_per_side.to_string().as_str())
                .map_err(|e| anyhow::anyhow!("Invalid size format: {}", e))?;

        debug!("Building limit order");
        let signable = self
            .client
            .limit_order()
            .token_id(token_id)
            .price(poly_price)
            .size(poly_size)
            .side(ClobSide::Buy)
            .build()
            .await?;

        debug!("Signing order");
        let signed = self
            .client
            .sign(
                &self.signer.clone().with_chain_id(Some(POLYGON)),
                signable,
            )
            .await?;

        debug!("Posting order");
        let response = self.client.post_order(signed).await?;

        if let Some(ref error_msg) = response.error_msg {
            if !error_msg.is_empty() {
                warn!(error_msg, order_id = %response.order_id, "Order posted with error message");
            }
        }

        info!(order_id = %response.order_id, "Order posted successfully");
        Ok(response.order_id)
    }

    fn build_snapshot(&self, market_id: &str, state: &MarketState) -> Option<MarketSnapshot> {
        let open_price = match state.binance_open_price {
            Some(price) => price,
            None => {
                debug!(
                    market_id,
                    asset = %state.info.asset,
                    "Skipping snapshot: no open price"
                );
                return None;
            }
        };

        let current_price = match self.chainlink_prices.get(&state.info.asset) {
            Some(&price) => price,
            None => {
                debug!(
                    market_id,
                    asset = %state.info.asset,
                    "Skipping snapshot: no chainlink price"
                );
                return None;
            }
        };

        let pair = state.pair.read();
        let up_ask = pair.latest_up_ask();
        let down_ask = pair.latest_down_ask();

        match (up_ask, down_ask) {
            (Some(up), Some(down)) => Some(MarketSnapshot {
                asset: state.info.asset.clone(),
                market_id: market_id.to_string(),
                open_price,
                current_price,
                pm_up_ask: up,
                pm_down_ask: down,
                elapsed_pct: state.elapsed_pct(Utc::now()),
            }),
            _ => {
                debug!(
                    market_id,
                    asset = %state.info.asset,
                    up_ask = ?up_ask,
                    down_ask = ?down_ask,
                    "Skipping snapshot: missing PM asks"
                );
                None
            }
        }
    }

    fn log_status(&self) {
        let now = Utc::now();
        let est_time = now.with_timezone(&New_York).format("%H:%M:%S");

        info!(
            time_est = %est_time,
            markets = self.markets.len(),
            active_positions = self.positions.values().filter(|p| !p.is_complete()).count(),
            complete_positions = self.positions.values().filter(|p| p.is_complete()).count(),
            "Status update"
        );

        for (market_id, state) in &self.markets {
            let pair = state.pair.read();
            let current_price = self
                .chainlink_prices
                .get(&state.info.asset)
                .copied()
                .unwrap_or(dec!(0));

            let move_bps = state.binance_open_price
                .filter(|open| !open.is_zero())
                .map(|open| ((current_price - open) / open) * dec!(10000));

            let position_status = match self.positions.get(market_id) {
                Some(p) if p.is_complete() => {
                    let profit = p.profit().unwrap_or(dec!(0));
                    format!("COMPLETE cost={:.3} profit={:+.3}", p.total_cost(), profit)
                }
                Some(p) => {
                    format!(
                        "ASYNC {:?}@{:.3} age={}s",
                        p.first_side,
                        p.first_price,
                        p.age_seconds(now)
                    )
                }
                None => "IDLE".to_string(),
            };

            info!(
                asset = %state.info.asset.to_uppercase(),
                elapsed_pct = format!("{:.1}%", state.elapsed_pct(now) * 100.0),
                move_bps = move_bps.map(|b| format!("{:+.1}", b)).unwrap_or_else(|| "---".to_string()),
                up_ask = pair.rest_up_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                down_ask = pair.rest_down_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                position = %position_status,
                "Market status"
            );
        }
    }

    #[instrument(skip(self))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");

        let all_markets = match self.market_cache.get_markets(Utc::now()).await {
            Ok(markets) => markets,
            Err(e) => {
                error!(error = %e, "Failed to fetch markets from cache");
                return;
            }
        };

        let now = Utc::now();
        let active_markets: Vec<_> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

        info!(
            total_active = active_markets.len(),
            "Filtering to one market per asset"
        );

        // Select one market per asset (first encountered)
        let mut selected: HashMap<String, _> = HashMap::new();
        for market in active_markets {
            selected.entry(market.asset.clone()).or_insert(market);
        }

        info!(selected_count = selected.len(), "Markets selected");

        self.markets.clear();
        self.trading_pairs.clear();

        for (asset, info) in selected {
            let market_id = info.id.clone();
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            let open_time = Utc::now();

            let open_price = self.fetch_open_price_with_retry(&asset, open_time).await;

            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol: String::new(),
                start_time: open_time,
                end_time: open_time + DEFAULT_WINDOW_DURATION,
                binance_open_price: open_price,
            };

            info!(
                market_id = %market_id,
                asset = %asset,
                open_price = ?open_price,
                end_time = %state.end_time,
                "Market registered"
            );

            self.markets.insert(market_id.clone(), state);
            self.trading_pairs.insert(market_id, pair);
        }

        self.current_window_end = self.markets.values().map(|s| s.end_time).min();

        info!(
            market_count = self.markets.len(),
            window_end = ?self.current_window_end,
            "Market discovery complete"
        );
    }

    async fn fetch_open_price_with_retry(
        &self,
        asset: &str,
        open_time: DateTime<Utc>,
    ) -> Option<Decimal> {
        for attempt in 1..=OPEN_PRICE_MAX_RETRIES {
            debug!(
                asset,
                attempt,
                max_retries = OPEN_PRICE_MAX_RETRIES,
                "Fetching open price"
            );

            let result = tokio::time::timeout(
                OPEN_PRICE_FETCH_TIMEOUT,
                self.chainlink.get_price_at(asset, open_time),
            )
            .await;

            match result {
                Ok(Ok(price_data)) => {
                    info!(
                        asset,
                        price = %price_data.price,
                        "Open price fetched successfully"
                    );
                    return Some(price_data.price);
                }
                Ok(Err(e)) => {
                    warn!(
                        asset,
                        attempt,
                        error = %e,
                        "Open price fetch failed"
                    );
                }
                Err(_) => {
                    warn!(
                        asset,
                        attempt,
                        timeout_secs = OPEN_PRICE_FETCH_TIMEOUT.as_secs(),
                        "Open price fetch timed out"
                    );
                }
            }

            if attempt < OPEN_PRICE_MAX_RETRIES {
                tokio::time::sleep(OPEN_PRICE_RETRY_DELAY).await;
            }
        }

        warn!(
            asset,
            attempts = OPEN_PRICE_MAX_RETRIES,
            "Failed to fetch open price after all retries"
        );
        None
    }

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    async fn update_prices(&mut self) {
        // Collect market info first to avoid borrow conflicts
        let market_info: Vec<_> = self
            .markets
            .values()
            .map(|state| {
                (
                    state.info.id.clone(),
                    state.info.asset.clone(),
                    state.info.up_token_id.clone(),
                    state.info.down_token_id.clone(),
                )
            })
            .collect();

        // Update Chainlink prices
        for (_, asset, _, _) in &market_info {
            match self.chainlink.get_latest_price(asset).await {
                Ok(price_data) => {
                    self.chainlink_prices.insert(asset.clone(), price_data.price);
                }
                Err(e) => {
                    debug!(
                        asset = %asset,
                        error = %e,
                        "Failed to fetch Chainlink price"
                    );
                }
            }
        }

        // Update Polymarket prices
        let client = reqwest::Client::new();
        for (market_id, _, up_token_id, down_token_id) in &market_info {
            // Up token
            match crate::ws::clob::fetch_token_price(&client, up_token_id).await {
                Ok(Some((bid, ask))) => {
                    if let Some(pair) = self.trading_pairs.get(market_id) {
                        let mut p = pair.write();
                        p.rest_up_bid = Some(bid);
                        p.rest_up_ask = Some(ask);
                        p.last_rest_update_ms = crate::models::now_ms();
                    }
                }
                Ok(None) => {
                    debug!(
                        market_id,
                        token_id = %up_token_id,
                        side = "up",
                        "No price data returned"
                    );
                }
                Err(e) => {
                    debug!(
                        market_id,
                        token_id = %up_token_id,
                        side = "up",
                        error = %e,
                        "Failed to fetch token price"
                    );
                }
            }

            // Down token
            match crate::ws::clob::fetch_token_price(&client, down_token_id).await {
                Ok(Some((bid, ask))) => {
                    if let Some(pair) = self.trading_pairs.get(market_id) {
                        let mut p = pair.write();
                        p.rest_down_bid = Some(bid);
                        p.rest_down_ask = Some(ask);
                        p.last_rest_update_ms = crate::models::now_ms();
                    }
                }
                Ok(None) => {
                    debug!(
                        market_id,
                        token_id = %down_token_id,
                        side = "down",
                        "No price data returned"
                    );
                }
                Err(e) => {
                    debug!(
                        market_id,
                        token_id = %down_token_id,
                        side = "down",
                        error = %e,
                        "Failed to fetch token price"
                    );
                }
            }
        }
    }

    fn should_rotate_markets(&self, now: DateTime<Utc>) -> bool {
        self.current_window_end.map_or(true, |end| now >= end)
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}