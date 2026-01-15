use crate::config::Config;
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderOverrides, LadderState};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};
use crate::poller::{InventoryLedger, MarketSide, spawn_order_feed};
use crate::price_feed::{PriceCache, spawn_price_feed};
use crate::trend_window::TrendWindow;
use crate::training_log::{TrainingLogger, TradeRecord, TradeAction, AdaptiveParams, CompletedTrade};
use crate::config::ASSETS_BY_NAME;
use chrono::Timelike;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::auth::Credentials;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{
    AssetType, OrderStatusType, Side as ClobSide, SignatureType, SignedOrder,
};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::types::{Address, B256, Decimal as PolyDecimal};
use rust_decimal_macros::dec;
use alloy::primitives::U256;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;
type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(50);  /* Reduced from 200ms for faster reaction */
const BALANCE_REFRESH_SECS: u64 = 30;

const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";

#[derive(Debug, Clone)]
struct PositionSnapshot {
    last_log: Instant,
    up_shares: Decimal,
    down_shares: Decimal,
    pending_up: Decimal,
    pending_down: Decimal,
}

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
    last_model_cancel: HashMap<String, Instant>,
    last_balance_refresh: HashMap<String, Instant>,
    last_position_log: HashMap<String, PositionSnapshot>,

    ledger: Arc<RwLock<InventoryLedger>>,
    price_cache: Arc<RwLock<PriceCache>>,
    _order_feed: Option<tokio::task::JoinHandle<()>>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    active_market_ids: Arc<RwLock<Vec<String>>>,
    ws_client: Arc<AuthenticatedWsClient>,

    ladder_engine: LadderEngine,
    ladder_state: LadderState,
    trend_windows: HashMap<String, TrendWindow>,
    trend_log_state: HashMap<String, TrendLogState>,
    last_missing_price_log: HashMap<String, Instant>,
    last_dry_run_log: HashMap<String, Instant>,
    
    /* Training and adaptive learning */
    training_logger: TrainingLogger,
    adaptive_params: AdaptiveParams,
    pending_orders: HashMap<String, TradeRecord>,  /* order_id -> record */
    last_recorded_pairs: HashMap<String, Decimal>,  /* market_id -> last recorded min_pair_size */
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let market_cache = MarketCache::new(config.target_assets.clone(), config.target_duration);
        let price_cache = Arc::new(RwLock::new(PriceCache::default()));

        let ladder_config = LadderConfig {
            size_per_level: config.order_size,
            target_per_side: config.shares_target_per_side,
            max_pair_cost: config.target_total_cost,
            top_offset: config.maker_price_offset,
            ..Default::default()
        };
        let ladder_engine = LadderEngine::new(ladder_config);
        let ladder_state = LadderState::default();

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));

        let addr = Address::from_str(config.polymarket_proxy_address.trim())
            .map_err(|e| anyhow::anyhow!("Invalid POLYMARKET_PROXY_ADDRESS: {}", e))?;

        let rest_client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?;
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
            last_model_cancel: HashMap::new(),
            last_balance_refresh: HashMap::new(),
            last_position_log: HashMap::new(),
            active_market_ids,
            ledger,
            _order_feed: None,
            ws_client,
            price_cache,
            _price_feed: None,
            ladder_engine,
            ladder_state,
            trend_windows: HashMap::new(),
            trend_log_state: HashMap::new(),
            last_missing_price_log: HashMap::new(),
            last_dry_run_log: HashMap::new(),
            training_logger: TrainingLogger::default(),
            adaptive_params: AdaptiveParams::default(),
            pending_orders: HashMap::new(),
            last_recorded_pairs: HashMap::new(),
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;
        if self.market_count() == 0 {
            warn!("No markets discovered, will retry in main loop");
        } else {
            for state in self.markets().values() {
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

        self.last_balance_refresh
            .insert(market_id.to_string(), now);

        match self.fetch_token_balances(up_token_id, down_token_id).await {
            Ok((up_bal, down_bal)) => {
                self.ledger
                    .write()
                    .sync_position(market_id.to_string(), up_bal, down_bal);
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

        /* Fetch both balances in parallel for speed */
        let (up_resp, down_resp) = tokio::join!(
            self.client.balance_allowance(up_req),
            self.client.balance_allowance(down_req)
        );
        let up_resp = up_resp?;
        let down_resp = down_resp?;

        let up_raw: Decimal = up_resp.balance;
        let down_raw: Decimal = down_resp.balance;

        debug!(
            up_raw = %up_raw,
            down_raw = %down_raw,
            "Raw balances from API"
        );

        let up_bal = up_raw / dec!(1_000_000);
        let down_bal = down_raw / dec!(1_000_000);

        debug!(
            up_bal = %up_bal,
            down_bal = %down_bal,
            "fetch_token_balances returning"
        );

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
            let market_symbol = market_symbol(&info);
            let market_id_for_ledger = market_id.clone();
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                start_time: info.start_time,
                end_time: info.end_time,
            };

            info!(
                market = %market_symbol,
                asset = %asset,
                end_time = %state.end_time,
                "Market registered"
            );

            self.markets.insert(market_id.clone(), state);
            self.trading_pairs.insert(market_id, pair);
            self.ledger
                .write()
                .register_market(market_id_for_ledger, market_symbol);
        }
        let mut removed_markets: Vec<(String, String, String)> = Vec::new();
        for (market_id, up_token_id, down_token_id) in previous_markets.iter() {
            if !self.markets.contains_key(market_id) {
                removed_markets.push((
                    market_id.clone(),
                    up_token_id.clone(),
                    down_token_id.clone(),
                ));
                self.ladder_state.clear_market(market_id);
                self.trend_windows.remove(market_id);
                self.trend_log_state.remove(market_id);
                self.last_missing_price_log.remove(market_id);
                self.last_dry_run_log.remove(market_id);
                self.last_order_by_market.remove(market_id);
            }
        }

        if !removed_markets.is_empty() {
            let cancel_ids: Vec<String> = {
                let ledger = self.ledger.read();
                removed_markets
                    .iter()
                    .flat_map(|(market_id, _, _)| {
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
                        info!(count = cancelled.len(), "Cancelled orders for rolled-over markets");
                        self.ledger.write().mark_orders_cancelled(&cancelled);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to cancel orders for rolled-over markets");
                    }
                }
            }
            let mut ledger = self.ledger.write();
            for (market_id, _, _) in removed_markets {
                ledger.unregister_market(&market_id);
            }
        }

        /* Update active market IDs after processing all markets */
        {
            let mut ids = self.active_market_ids.write();
            *ids = self.markets.keys().cloned().collect();
        }

        let token_ids: Vec<String> = self
            .markets
            .values()
            .flat_map(|m| vec![m.info.up_token_id.clone(), m.info.down_token_id.clone()])
            .collect();

        if let Some(handle) = self._price_feed.take() {
            handle.abort();
        }

        if let Some(handle) = self._order_feed.take() {
            handle.abort();
        }

        for (market_id, state) in &self.markets {
            let market_symbol = market_symbol(&state.info);
            match self
                .fetch_token_balances(&state.info.up_token_id, &state.info.down_token_id)
                .await
            {
                Ok((up_bal, down_bal)) => {
                    /* Try to get current prices to estimate cost for pre-existing positions */
                    let price_cache = self.price_cache.read();
                    let up_mid = price_cache.get(&state.info.up_token_id)
                        .map(|(bid, ask)| (bid + ask) / dec!(2));
                    let down_mid = price_cache.get(&state.info.down_token_id)
                        .map(|(bid, ask)| (bid + ask) / dec!(2));
                    drop(price_cache);
                    
                    self.ledger
                        .write()
                        .sync_position_with_prices(market_id.clone(), up_bal, down_bal, up_mid, down_mid);
                    info!(
                        market = %market_symbol,
                        up = %up_bal,
                        down = %down_bal,
                        "Position loaded"
                    );
                }
                Err(e) => {
                    warn!(market = %market_symbol, error = %e, "Failed to fetch position");
                }
            }
        }

        let order_market_ids: Vec<B256> = self
            .markets
            .values()
            .filter_map(|state| state.info.condition_id)
            .collect();
        if order_market_ids.is_empty() {
            warn!("No condition ids available for order WebSocket subscription");
        } else {
            self._order_feed = Some(spawn_order_feed(
                self.ws_client.clone(),
                self.ledger.clone(),
                order_market_ids,
            ));
        }

        match spawn_price_feed(WS_SUB_URL, token_ids, self.price_cache.clone()) {
            Ok(handle) => {
                self._price_feed = Some(handle);
                info!("Price feed connected");
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
        let total_seconds = now.minute() * 60 + now.second();

        /* Refresh in the last 30 seconds of each 15-minute window (14:00-14:30) */
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

    fn news_guard_active(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        let window = chrono::Duration::seconds(self.config.news_guard_window_secs as i64);
        self.config
            .news_event_times
            .iter()
            .any(|event| {
                let delta = *event - now;
                delta.num_seconds().abs() <= window.num_seconds()
            })
    }

    async fn scan(&mut self) {
        let market_ids: Vec<String> = self.markets.keys().cloned().collect();

        for market_id in market_ids {
            let state = match self.markets.get(&market_id) {
                Some(s) => s,
                None => continue,
            };
            let market_symbol = market_symbol(&state.info);
            let (up_token_id, down_token_id) =
                (state.info.up_token_id.clone(), state.info.down_token_id.clone());

            self.refresh_positions(
                &market_id,
                &up_token_id,
                &down_token_id,
                &market_symbol,
            )
            .await;

            let price_snapshot = {
                let cache = self.price_cache.read();
                let now_ms = Utc::now().timestamp_millis() as u64;
                match (
                    cache.get_with_age(&up_token_id, now_ms),
                    cache.get_with_age(&down_token_id, now_ms),
                ) {
                    (Some((up_bid, up_ask, up_age)), Some((down_bid, down_ask, down_age))) => {
                        Some((up_bid, up_ask, down_bid, down_ask, up_age, down_age))
                    }
                    _ => None,
                }
            };

            let (up_ask, down_ask, effective_target, mut overrides, summary, up_bid, down_bid) =
                match price_snapshot {
                Some((up_bid, up_ask, down_bid, down_ask, up_age, down_age)) => {
                    let now_ms = Utc::now().timestamp_millis();
                    let mut status = TrendStatus::Ready;
                    let mut summary = None;
                    let mut reason = "ok";
                    let mut extra = TrendLogExtra::default();
                    extra.max_age_ms = Some(up_age.max(down_age) as i64);
                    let news_guard_active = self.news_guard_active(Utc::now());

                    let (ranges, span_ms, sample_count, effective_target, overrides) = {
                        let window = self
                            .trend_windows
                            .entry(market_id.clone())
                            .or_default();
                        window.record(
                            now_ms,
                            up_bid,
                            up_ask,
                            down_bid,
                            down_ask,
                            self.config.trend_window_secs,
                        );

                        let ranges = window.ranges();
                        let mid_start_end = window.mid_start_end();
                        if let Some((start, end)) = mid_start_end {
                            extra.start_mid = Some(start);
                            extra.end_mid = Some(end);
                        }

                        if !window.is_ready(self.config.trend_window_secs) {
                            status = TrendStatus::NotReady;
                            reason = "not_ready";
                        } else {
                            let up_spread = up_ask - up_bid;
                            let down_spread = down_ask - down_bid;
                            let max_spread = up_spread.max(down_spread);
                            extra.up_spread = up_spread;
                            extra.down_spread = down_spread;

                            if max_spread > self.config.max_side_spread {
                                status = TrendStatus::OutOfRange;
                                reason = "spread";
                            }

                            let max_age_ms = extra.max_age_ms.unwrap_or(0);
                            if max_age_ms > self.config.max_price_age_ms {
                                status = TrendStatus::OutOfRange;
                                reason = "stale";
                            }

                            if news_guard_active {
                                status = TrendStatus::OutOfRange;
                                reason = "news_guard";
                            }

                            let mid_up = (up_bid + up_ask) / dec!(2);
                            let mid_up_f64 = mid_up.to_f64().unwrap_or(0.5);
                            let liquidity = mid_up_f64 * (1.0 - mid_up_f64);
                            let scale = (liquidity / 0.25)
                                .max(self.config.min_liquidity_scale.to_f64().unwrap_or(0.2));
                            let effective_max_range = self.config.trend_max_range
                                * Decimal::from_f64(scale).unwrap_or(self.config.trend_max_range);
                            extra.effective_max_range = Some(effective_max_range);

                            let range_exceeded = !window.is_within_range(effective_max_range);

                            if status == TrendStatus::Ready {
                                summary = window.rn_jd_summary();
                                if let Some(s) = summary {
                                    if s.jump_intensity >= self.config.lambda_pause_threshold {
                                        status = TrendStatus::OutOfRange;
                                        reason = "lambda_spike";
                                    }
                                }
                            }

                            if status == TrendStatus::Ready && range_exceeded {
                                if let Some((start, end)) = mid_start_end {
                                    let directional_move = (end - start).abs();
                                    let directional_move_dec =
                                        Decimal::from_f64(directional_move).unwrap_or_default();
                                    if directional_move_dec > self.config.directional_move_threshold {
                                        status = TrendStatus::OutOfRange;
                                        reason = "jump";
                                    } else if directional_move_dec <= self.config.high_vol_reversion_threshold {
                                        extra.high_vol_widen = true;
                                        reason = "high_vol";
                                    } else {
                                        status = TrendStatus::OutOfRange;
                                        reason = "range";
                                    }
                                } else {
                                    status = TrendStatus::OutOfRange;
                                    reason = "range";
                                }
                            }
                        }

                        let mid_up = (up_bid + up_ask) / dec!(2);
                        let swing_zone = mid_up >= self.config.swing_zone_low
                            && mid_up <= self.config.swing_zone_high;
                        extra.swing_zone = swing_zone;
                        let effective_target = if swing_zone {
                            self.config.shares_target_per_side * self.config.swing_zone_target_factor
                        } else {
                            self.config.shares_target_per_side
                        };

                        let (span_ms, sample_count) = window.span_ms_and_count();
                        let overrides = build_ladder_overrides(
                            summary,
                            up_bid,
                            up_ask,
                            down_bid,
                            down_ask,
                            mid_start_end,
                            self.config.edge_threshold,
                            self.config.size_scale_min,
                            self.config.size_scale_max,
                            self.config.widen_factor,
                            self.config.tick_size,
                            self.config.pinned_low,
                            self.config.pinned_high,
                            self.config.drift_flicker_threshold,
                            extra.high_vol_widen,
                            self.ladder_engine.config().levels,
                            self.config.target_total_cost,
                            self.adaptive_params.min_conviction,  /* Learned threshold */
                        );
                        extra.size_multiplier = overrides.size_multiplier;
                        extra.spacing_multiplier = overrides.spacing_multiplier;
                        extra.extra_offset = overrides.extra_offset;
                        extra.up_price_cap = overrides.up_price_cap;
                        extra.down_price_cap = overrides.down_price_cap;
                        extra.max_levels = overrides.max_levels;
                        extra.conviction = summary.map(|s| s.vol_metrics.conviction).unwrap_or(0.0);
                        (ranges, span_ms, sample_count, effective_target, overrides)
                    };

                    if self.should_log_trend(&market_id, status) {
                        let max_range = match status {
                            TrendStatus::OutOfRange => Some(self.config.trend_max_range),
                            _ => None,
                        };
                        debug!(
                            market = %market_symbol,
                            "{}",
                            format_trend_log(
                                reason,
                                up_bid,
                                up_ask,
                                down_bid,
                                down_ask,
                                ranges,
                                span_ms,
                                sample_count,
                                max_range,
                                extra,
                                summary,
                            )
                        );
                    }

                    if status != TrendStatus::Ready {
                        /*
                         * CONSERVATIVE APPROACH: On OutOfRange, just pause new orders.
                         * Only cancel if orders are on the EXPENSIVE side (price rising away from us).
                         * This lets cheap-side orders stay and potentially fill.
                         * 
                         * Logic is SYMMETRIC - we just compare which side costs more.
                         */
                        
                        /* Still check for completed pairs during pauses (learning continues) */
                        self.check_completed_pairs(&market_id);
                        
                        let open_orders = self.ledger.read().open_orders_for_market(&market_id);
                        
                        if !open_orders.is_empty() && self.should_model_cancel(&market_id) {
                            /* Determine which side is "expensive" based on current prices, not labels */
                            let up_price = up_ask.to_f64().unwrap_or(0.5);
                            let down_price = down_ask.to_f64().unwrap_or(0.5);
                            
                            /* The expensive side is whichever is closer to $1 */
                            let expensive_side = if up_price > down_price {
                                Some(MarketSide::Up)
                            } else if down_price > up_price {
                                Some(MarketSide::Down)
                            } else {
                                None  /* Equal - don't cancel either */
                            };
                            
                            /* Only cancel orders on the expensive side */
                            let cancel_ids: Vec<String> = open_orders
                                .into_iter()
                                .filter(|o| expensive_side == Some(o.side))
                                .map(|o| o.order_id)
                                .collect();
                            
                            if !cancel_ids.is_empty() {
                                let side_str = expensive_side
                                    .map(|s| format!("{:?}", s))
                                    .unwrap_or_else(|| "none".to_string());
                                debug!(
                                    market = %market_symbol,
                                    reason = %reason,
                                    expensive_side = %side_str,
                                    count = cancel_ids.len(),
                                    "Pausing: cancelling expensive side only"
                                );
                                match self.cancel_orders(&cancel_ids).await {
                                    Ok(cancelled) => {
                                        self.ledger.write().mark_orders_cancelled(&cancelled);
                                    }
                                    Err(e) => {
                                        error!(
                                            market = %market_symbol,
                                            error = %e,
                                            "Failed to cancel orders"
                                        );
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    (up_ask, down_ask, effective_target, overrides, summary, up_bid, down_bid)
                }
                None => {
                    let should_log = self.should_log_missing_price(&market_id);
                    if should_log {
                        info!(market = %market_symbol, "Waiting for price data");
                    }
                    continue;
                }
            };

            let should_reladder = self.ladder_state.should_reladder(
                &market_id,
                up_ask,
                down_ask,
                self.ladder_engine.config().reladder_threshold,
            );

            if !should_reladder && !self.cooldown_elapsed(&market_id) {
                continue;
            }

            /* Get position and open orders */
            let (pos, open_orders) = {
                let ledger = self.ledger.read();
                (
                    ledger.effective_position(&market_id),
                    ledger.open_orders_for_market(&market_id),
                )
            };

            let pending_up: Decimal = open_orders
                .iter()
                .filter(|o| o.side == MarketSide::Up)
                .map(|o| o.remaining_size)
                .sum();
            let pending_down: Decimal = open_orders
                .iter()
                .filter(|o| o.side == MarketSide::Down)
                .map(|o| o.remaining_size)
                .sum();
            let total_up = pos.up_shares + pending_up;
            let total_down = pos.down_shares + pending_down;
            let imbalance = total_up - total_down;

            apply_imbalance_override(
                &mut overrides,
                summary,
                up_bid,
                down_bid,
                imbalance,
                self.ladder_engine.config().max_imbalance,
                self.config.edge_threshold,
            );
            
            /*
             * PASS FILL PRICES AND CONVICTION TO OVERRIDES
             * 
             * This enables:
             * - Single-side rebalancing with combined cost validation
             * - Conviction-based aggressive pricing (high conviction → bid closer to ask)
             */
            {
                let ledger = self.ledger.read();
                let (avg_up, avg_down) = ledger.average_fill_prices(&market_id);
                overrides.avg_up_cost = avg_up;
                overrides.avg_down_cost = avg_down;
                /* Conviction was already set in build_ladder_overrides from vol_metrics */
                overrides.conviction = summary
                    .map(|s| s.vol_metrics.conviction)
                    .unwrap_or(0.5);
            }

            if self.should_log_position(
                &market_id,
                PositionSnapshot {
                    last_log: Instant::now(),
                    up_shares: pos.up_shares,
                    down_shares: pos.down_shares,
                    pending_up,
                    pending_down,
                },
            ) {
                let imbalance_abs = imbalance.abs();
                let imbalance_str = if imbalance_abs >= self.ladder_engine.config().max_imbalance {
                    format!(" imbalance={:.4}", imbalance)
                } else {
                    String::new()
                };
                
                /* Get average fill prices for logging */
                let ledger = self.ledger.read();
                let (avg_up, avg_down) = ledger.average_fill_prices(&market_id);
                drop(ledger);
                
                let avg_str = match (avg_up, avg_down) {
                    (Some(u), Some(d)) => {
                        let combined = u + d;
                        let profit = (Decimal::ONE - combined) * dec!(100);
                        format!(" avg[up={:.2}¢ down={:.2}¢ combined={:.2}¢ profit={:.1}¢]", 
                            u * dec!(100), d * dec!(100), combined * dec!(100), profit)
                    }
                    (Some(u), None) => format!(" avg[up={:.2}¢ down=?]", u * dec!(100)),
                    (None, Some(d)) => format!(" avg[up=? down={:.2}¢]", d * dec!(100)),
                    (None, None) => String::new(),
                };
                
                debug!(
                    market = %market_symbol,
                    "position up={:.4} down={:.4} pending(up={:.4} down={:.4}) total(up={:.4} down={:.4}){}{} target={:.2}",
                    pos.up_shares,
                    pos.down_shares,
                    pending_up,
                    pending_down,
                    total_up,
                    total_down,
                    imbalance_str,
                    avg_str,
                    effective_target
                );
            }

            let plan = self
                .ladder_engine
                .compute_ladder_with_target(
                    up_ask,
                    down_ask,
                    &pos,
                    &open_orders,
                    Some(effective_target),
                    Some(overrides),
                );

            if plan.cancellations.is_empty() && plan.orders.is_empty() {
                continue;
            }

            /* Cancel stale orders first */
            if !plan.cancellations.is_empty() {
                match self.cancel_orders(&plan.cancellations).await {
                    Ok(cancelled) => {
                        if cancelled.is_empty() {
                            debug!(market = %market_symbol, "No stale orders cancelled");
                        } else {
                            info!(
                                market = %market_symbol,
                                count = cancelled.len(),
                                "Cancelled stale orders"
                            );
                        }
                        self.ledger.write().mark_orders_cancelled(&cancelled);
                    }
                    Err(e) => {
                        error!(market = %market_symbol, error = %e, "Failed to cancel orders");
                    }
                }
            }

            if !plan.orders.is_empty() {
                if self.config.dry_run {
                    let should_log = self.should_log_dry_run(&market_id);
                    if should_log {
                        info!(
                            market = %market_symbol,
                            count = plan.orders.len(),
                            "Dry run: skipping order placement"
                        );
                        for order in &plan.orders {
                            info!(
                                market = %market_symbol,
                                side = ?order.side,
                                price = %order.price,
                                size = %order.size,
                                "Dry run order"
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
                                if resp
                                    .error_msg
                                    .as_ref()
                                    .map(|s| s.is_empty())
                                    .unwrap_or(true)
                                {
                                    self.ledger.write().record_order_placed(
                                        resp.order_id.clone(),
                                        market_id.clone(),
                                        order.side,
                                        order.size,
                                        order.price,
                                    );
                                    
                                    /* Log for training */
                                    let mut record = TrainingLogger::create_record(
                                        &market_id,
                                        up_bid,
                                        up_ask,
                                        down_bid,
                                        down_ask,
                                        summary.as_ref(),
                                    );
                                    record.action = TradeAction::PlaceOrder;
                                    record.order_side = Some(format!("{:?}", order.side));
                                    record.order_price = order.price.to_f64();
                                    record.order_size = order.size.to_f64();
                                    self.training_logger.log(&record);
                                    self.pending_orders.insert(resp.order_id.clone(), record);
                                    
                                    placed += 1;
                                } else {
                                    warn!(
                                        market = %market_symbol,
                                        order_id = %resp.order_id,
                                        error = ?resp.error_msg,
                                        "Order rejected"
                                    );
                                }
                            }

                            info!(
                                market = %market_symbol,
                                up_ask = %up_ask,
                                down_ask = %down_ask,
                                cancelled = plan.cancellations.len(),
                                placed,
                                "Ladder updated"
                            );

                            self.ladder_state
                                .record_ladder(market_id.clone(), up_ask, down_ask);
                        }
                        Err(e) => {
                            error!(market = %market_symbol, error = %e, "Batch order submission failed");
                        }
                    }
                }
            }

            self.last_order_by_market
                .insert(market_id.clone(), Instant::now());
                
            /* Check for completed pairs and update adaptive params */
            self.check_completed_pairs(&market_id);
        }
    }
    
    /*
     * Check for completed trade pairs and update adaptive parameters.
     * A "completed pair" is when we've bought both Up and Down shares.
     * 
     * Only records profit for NEW pairs (not already recorded).
     * 
     * Profit calculation:
     *   total_cost = up_cost + down_cost (what we paid)
     *   payout = min_shares * $1 (what we get when market resolves)
     *   profit = payout - total_cost
     * 
     * Example: 10 Up @ 90¢ + 10 Down @ 5¢
     *   cost = $9.00 + $0.50 = $9.50
     *   payout = 10 * $1 = $10.00
     *   profit = $10.00 - $9.50 = $0.50 = 50¢
     */
    fn check_completed_pairs(&mut self, market_id: &str) {
        let ledger = self.ledger.read();
        let position = ledger.effective_position(market_id);
        drop(ledger);
        
        let up_shares = position.up_shares;
        let down_shares = position.down_shares;
        let up_cost = position.up_cost;
        let down_cost = position.down_cost;
        
        /* Check if we have complete pairs with tracked costs */
        let min_pair_size = up_shares.min(down_shares);
        if min_pair_size < dec!(1) || up_cost <= Decimal::ZERO || down_cost <= Decimal::ZERO {
            return;
        }
        
        /* Only record NEW pairs (not already counted) */
        let last_recorded = self.last_recorded_pairs
            .get(market_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        
        let new_pairs = min_pair_size - last_recorded;
        if new_pairs < dec!(1) {
            return;  /* No new complete pairs */
        }
        
        /* Calculate profit for the NEW pairs only */
        /* 
         * For the new pairs, we use the proportional cost:
         * new_up_cost = up_cost * (new_pairs / up_shares)
         * new_down_cost = down_cost * (new_pairs / down_shares)
         * 
         * But since we want avg prices, it's simpler:
         * avg_up = up_cost / up_shares
         * avg_down = down_cost / down_shares
         * profit_per_share = 1 - (avg_up + avg_down)
         * total_profit = profit_per_share * new_pairs
         */
        let avg_up_price = up_cost / up_shares;
        let avg_down_price = down_cost / down_shares;
        let combined_cost = avg_up_price + avg_down_price;
        let profit_per_share = Decimal::ONE - combined_cost;
        
        /* TOTAL profit in cents for the new pairs */
        let total_profit_cents = (profit_per_share * new_pairs * dec!(100)).to_f64().unwrap_or(0.0);
        
        /* Get current model state for learning */
        let (conviction, momentum, calmness) = self.trend_windows
            .get_mut(market_id)
            .and_then(|w| w.rn_jd_summary())
            .map(|s| (s.vol_metrics.conviction, s.vol_metrics.momentum, s.vol_metrics.calmness))
            .unwrap_or((0.5, 0.0, 0.5));
        
        /* Record completed trade for adaptive learning */
        let trade = CompletedTrade {
            conviction_at_trade: conviction,
            momentum_at_trade: momentum,
            calmness_at_trade: calmness,
            combined_cost: combined_cost.to_f64().unwrap_or(1.0),
            profit_cents: total_profit_cents,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        };
        
        info!(
            market = %market_id,
            new_pairs = %new_pairs,
            avg_up = %format!("{:.2}¢", avg_up_price * dec!(100)),
            avg_down = %format!("{:.2}¢", avg_down_price * dec!(100)),
            combined = %format!("{:.2}¢", combined_cost * dec!(100)),
            profit = %format!("{:.2}¢", total_profit_cents),
            "Completed pairs recorded"
        );
        
        /* Log to jsonl for training data */
        self.training_logger.log_completed(&trade, market_id);
        
        self.adaptive_params.record_trade(trade);
        
        /* Update last recorded pairs */
        self.last_recorded_pairs.insert(market_id.to_string(), min_pair_size);
        
        /* Clear pending orders for this market */
        self.pending_orders.retain(|_, r| r.market_id != market_id);
    }

    fn should_log_missing_price(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        match self.last_missing_price_log.get(market_id) {
            Some(last) if now.duration_since(*last) < Duration::from_secs(5) => false,
            _ => {
                self.last_missing_price_log
                    .insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_trend(&mut self, market_id: &str, status: TrendStatus) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(5);
        match self.trend_log_state.get(market_id) {
            Some(state)
                if state.status == status && now.duration_since(state.last_log) < min_interval =>
            {
                false
            }
            _ => {
                self.trend_log_state.insert(
                    market_id.to_string(),
                    TrendLogState {
                        status,
                        last_log: now,
                    },
                );
                true
            }
        }
    }

    fn should_log_dry_run(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(10);
        match self.last_dry_run_log.get(market_id) {
            Some(last) if now.duration_since(*last) < min_interval => false,
            _ => {
                self.last_dry_run_log
                    .insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_position(&mut self, market_id: &str, snapshot: PositionSnapshot) -> bool {
        let min_interval = Duration::from_secs(5);
        let Some(prev) = self.last_position_log.get(market_id) else {
            self.last_position_log
                .insert(market_id.to_string(), snapshot);
            return true;
        };

        let changed = prev.up_shares != snapshot.up_shares
            || prev.down_shares != snapshot.down_shares
            || prev.pending_up != snapshot.pending_up
            || prev.pending_down != snapshot.pending_down;

        if changed || snapshot.last_log.duration_since(prev.last_log) >= min_interval {
            self.last_position_log
                .insert(market_id.to_string(), snapshot);
            return true;
        }

        false
    }

    fn should_model_cancel(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(5);
        match self.last_model_cancel.get(market_id) {
            Some(last) if now.duration_since(*last) < min_interval => false,
            _ => {
                self.last_model_cancel
                    .insert(market_id.to_string(), now);
                true
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
                "Placing order"
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
            info!(count = order_ids.len(), "Dry run: would cancel orders");
            return Ok(order_ids.to_vec());
        }

        /* The SDK expects a list of order IDs to cancel */
        let order_id_refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();

        match self.client.cancel_orders(&order_id_refs).await {
            Ok(response) => {
                if response.canceled.is_empty() && !response.not_canceled.is_empty() {
                    debug!(
                        requested = order_ids.len(),
                        cancelled = response.canceled.len(),
                        not_cancelled = response.not_canceled.len(),
                        "Batch cancel complete"
                    );
                } else {
                    info!(
                        requested = order_ids.len(),
                        cancelled = response.canceled.len(),
                        not_cancelled = response.not_canceled.len(),
                        "Batch cancel complete"
                    );
                }

                if !response.not_canceled.is_empty() {
                    let now = Instant::now();
                    let mut confirmed_closed = Vec::new();
                    let check_cooldown = Duration::from_secs(10);

                    for (order_id, reason) in response.not_canceled.iter() {
                        let should_check = match self.last_order_status_check.get(order_id) {
                            Some(last) => now.duration_since(*last) >= check_cooldown,
                            None => true,
                        };

                        if !should_check {
                            continue;
                        }

                        self.last_order_status_check
                            .insert(order_id.clone(), now);

                        match self.client.order(order_id).await {
                            Ok(order) => {
                                let is_live =
                                    matches!(order.status, OrderStatusType::Live);
                                self.ledger.write().apply_order_status(
                                    order_id,
                                    order.size_matched,
                                    is_live,
                                );
                                if !is_live {
                                    confirmed_closed.push(order_id.clone());
                                }
                                debug!(
                                    order_id = %order_id,
                                    status = ?order.status,
                                    reason = %reason,
                                    "Cancel rejected; order status checked"
                                );
                            }
                            Err(e) => {
                                debug!(
                                    order_id = %order_id,
                                    reason = %reason,
                                    error = %e,
                                    "Cancel rejected; order status lookup failed"
                                );
                            }
                        }
                    }

                    if !confirmed_closed.is_empty() {
                        self.ledger
                            .write()
                            .mark_orders_cancelled(&confirmed_closed);
                    }
                }

                Ok(response.canceled)
            }
            Err(e) => {
                error!(error = %e, "Batch cancel failed");
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

/* Format trend status for logging. Includes RN-JD model info when summary is provided. */
#[allow(clippy::too_many_arguments)]
fn format_trend_log(
    status: &str,
    up_bid: Decimal,
    up_ask: Decimal,
    down_bid: Decimal,
    down_ask: Decimal,
    ranges: crate::trend_window::TrendRanges,
    span_ms: i64,
    sample_count: usize,
    max_range: Option<Decimal>,
    extra: TrendLogExtra,
    summary: Option<crate::trend_window::RnJdSummary>,
) -> String {
    let max_range_str = max_range
        .map(format_decimal)
        .unwrap_or_else(|| "-".to_string());
    let effective_max_range_str = extra
        .effective_max_range
        .map(format_decimal)
        .unwrap_or_else(|| "-".to_string());
    let start_mid = extra.start_mid.unwrap_or_default();
    let end_mid = extra.end_mid.unwrap_or_default();
    let up_cap = extra.up_price_cap.map(format_decimal).unwrap_or_else(|| "-".to_string());
    let down_cap = extra.down_price_cap.map(format_decimal).unwrap_or_else(|| "-".to_string());
    
    let base = format!(
        "trend={} prices[up={}/{} down={}/{}] ranges[up={}/{} down={}/{} max={} eff_max={}] spreads[up={} down={}] age_ms={} swing_zone={} mid[start={:.5} end={:.5}] window[ms={} n={}]",
        status,
        format_decimal(up_bid),
        format_decimal(up_ask),
        format_decimal(down_bid),
        format_decimal(down_ask),
        format_decimal(ranges.up_bid),
        format_decimal(ranges.up_ask),
        format_decimal(ranges.down_bid),
        format_decimal(ranges.down_ask),
        max_range_str,
        effective_max_range_str,
        format_decimal(extra.up_spread),
        format_decimal(extra.down_spread),
        extra.max_age_ms.unwrap_or(-1),
        extra.swing_zone,
        start_mid,
        end_mid,
        span_ms,
        sample_count,
    );
    
    /* Include RN-JD model details when available */
    let rn_info = if let Some(s) = summary {
        format!(
            " rn[sigma={:.4} lambda={:.4} drift={:.4} up={:.4}/{:.4} down={:.4}/{:.4}]",
            s.sigma_b,
            s.jump_intensity,
            s.drift,
            s.rn_bid,
            s.rn_ask,
            1.0 - s.rn_ask,
            1.0 - s.rn_bid,
        )
    } else {
        String::new()
    };
    
    let overrides = format!(
        " CONVICTION={:.2} size_mult={} spacing_mult={} caps[up={} down={}] levels={} widen={}",
        extra.conviction,
        format_decimal(extra.size_multiplier),
        format_decimal(extra.spacing_multiplier),
        up_cap,
        down_cap,
        extra.max_levels.unwrap_or(0),
        extra.high_vol_widen,
    );
    
    format!("{}{}{}", base, rn_info, overrides)
}

fn format_decimal(value: Decimal) -> String {
    value.round_dp(5).to_string()
}

#[derive(Debug, Clone, Copy)]
struct TrendLogExtra {
    up_spread: Decimal,
    down_spread: Decimal,
    max_age_ms: Option<i64>,
    swing_zone: bool,
    high_vol_widen: bool,
    effective_max_range: Option<Decimal>,
    start_mid: Option<f64>,
    end_mid: Option<f64>,
    size_multiplier: Decimal,
    spacing_multiplier: Decimal,
    extra_offset: Decimal,
    up_price_cap: Option<Decimal>,
    down_price_cap: Option<Decimal>,
    max_levels: Option<usize>,
    conviction: f64,
}

impl Default for TrendLogExtra {
    fn default() -> Self {
        Self {
            up_spread: Decimal::ZERO,
            down_spread: Decimal::ZERO,
            max_age_ms: None,
            swing_zone: false,
            high_vol_widen: false,
            effective_max_range: None,
            start_mid: None,
            end_mid: None,
            size_multiplier: Decimal::ONE,
            spacing_multiplier: Decimal::ONE,
            extra_offset: Decimal::ZERO,
            up_price_cap: None,
            down_price_cap: None,
            max_levels: None,
            conviction: 0.0,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_ladder_overrides(
    summary: Option<crate::trend_window::RnJdSummary>,
    up_bid: Decimal,
    up_ask: Decimal,
    down_bid: Decimal,
    down_ask: Decimal,
    _mid_start_end: Option<(f64, f64)>,
    edge_threshold: Decimal,
    size_scale_min: Decimal,
    size_scale_max: Decimal,
    widen_factor: Decimal,
    tick_size: Decimal,
    pinned_low: Decimal,
    pinned_high: Decimal,
    drift_flicker_threshold: f64,
    high_vol_widen: bool,
    max_levels_default: usize,
    max_pair_cost: Decimal,
    min_conviction: f64,  /* Adaptive: learned from performance */
) -> LadderOverrides {
    let mut overrides = LadderOverrides::default();
    overrides.size_multiplier = Decimal::ONE;
    overrides.spacing_multiplier = Decimal::ONE;
    overrides.extra_offset = Decimal::ZERO;

    let Some(summary) = summary else {
        /* No summary yet - use conservative defaults */
        if high_vol_widen {
            overrides.spacing_multiplier = widen_factor;
            if widen_factor > Decimal::ONE {
                overrides.extra_offset = tick_size * (widen_factor - Decimal::ONE);
            }
        }
        return overrides;
    };

    /* 
     * Volatility-adaptive spread sizing:
     * - Tighter spreads in calm markets (more fills)
     * - Wider spreads when volatile (protection)
     * - Calmness ~1.0 → multiplier ~0.8
     * - Calmness ~0.0 → multiplier ~1.5
     */
    let vol_metrics = summary.vol_metrics;
    let vol_spread_mult = 1.5 - (0.7 * vol_metrics.calmness);
    
    /* If high_vol_widen flag is set (from range checks), apply additional widening */
    let base_mult = if high_vol_widen {
        vol_spread_mult * widen_factor.to_f64().unwrap_or(2.0)
    } else {
        vol_spread_mult
    };
    
    overrides.spacing_multiplier = Decimal::from_f64(base_mult.clamp(0.8, 2.5))
        .unwrap_or(Decimal::ONE);
    
    if overrides.spacing_multiplier > Decimal::ONE {
        let extra = tick_size * (overrides.spacing_multiplier - Decimal::ONE);
        overrides.extra_offset = extra;
    }

    let rn_mid = (summary.rn_bid + summary.rn_ask) / 2.0;
    let up_bid_f = up_bid.to_f64().unwrap_or(0.0);
    let up_ask_f = up_ask.to_f64().unwrap_or(1.0);
    let edge_threshold_f = edge_threshold.to_f64().unwrap_or(0.0025);
    let tick_size_f = tick_size.to_f64().unwrap_or(0.01);

    /*
     * CONVICTION-BASED SIZING
     * The conviction score (0-1) directly controls how much we trade:
     * - High conviction (>0.7) → trade at full size
     * - Medium conviction (0.4-0.7) → trade at reduced size  
     * - Low conviction (<0.4) → minimal size or skip
     * 
     * This replaces the complex edge/flicker heuristics with one clear number.
     */
    let conviction = vol_metrics.conviction;
    
    /* Minimum conviction to trade at all (adaptive, starts at 0.25) */
    let min_conviction_threshold = min_conviction;
    
    if conviction < min_conviction_threshold {
        /* Very low conviction: set caps to None (skip trading) */
        overrides.up_price_cap = None;
        overrides.down_price_cap = None;
        overrides.size_multiplier = size_scale_min;
        debug!(conviction = %format!("{:.2}", conviction), "Low conviction, skipping orders");
        return overrides;
    }
    
    /* Size scales with conviction: conviction 0.4 → 40% size, conviction 1.0 → 100% size */
    let size_scale = conviction.clamp(0.3, 1.0);
    let size_range = size_scale_max - size_scale_min;
    overrides.size_multiplier = size_scale_min + size_range * Decimal::from_f64(size_scale).unwrap_or(Decimal::ONE);
    
    /* Additional boost for pinned markets (near 0 or 100) with clear drift */
    let mid_up = ((up_bid + up_ask) / dec!(2)).to_f64().unwrap_or(0.5);
    let pinned_low_f = pinned_low.to_f64().unwrap_or(0.05);
    let pinned_high_f = pinned_high.to_f64().unwrap_or(0.95);
    if (mid_up <= pinned_low_f || mid_up >= pinned_high_f)
        && summary.drift.abs() >= drift_flicker_threshold
        && conviction > 0.5
    {
        overrides.size_multiplier = size_scale_max;
    }

    /* Price level calculation: use RN-JD fair value, validate against market */
    let rn_spread = (summary.rn_ask - summary.rn_bid).max(0.0);
    let raw_levels = (rn_spread / tick_size_f).ceil();
    let max_levels = if raw_levels.is_finite() && raw_levels > 0.0 {
        raw_levels as usize
    } else {
        1
    };
    overrides.max_levels = Some(max_levels.clamp(1, max_levels_default));

    /* 
     * Target bid price: use RN fair value but don't be too aggressive.
     * Place orders AT or BELOW the current market bid.
     * Returns None if we shouldn't buy that side (no edge).
     */
    let down_bid_f = down_bid.to_f64().unwrap_or(0.0);
    let down_ask_f = down_ask.to_f64().unwrap_or(1.0);
    
    let target_up = conservative_bid_price(
        rn_mid,
        up_bid_f,
        up_ask_f,
        edge_threshold_f,
        tick_size_f,
        vol_metrics.calmness,
    );
    
    let target_down_mid = 1.0 - rn_mid;
    let target_down = conservative_bid_price(
        target_down_mid,
        down_bid_f,
        down_ask_f,
        edge_threshold_f,
        tick_size_f,
        vol_metrics.calmness,
    );
    
    /* Log edge calculations for debugging */
    let up_edge = rn_mid - up_bid_f;
    let down_edge = target_down_mid - down_bid_f;
    let momentum_dir = if vol_metrics.momentum > 0.3 {
        "Up↑"
    } else if vol_metrics.momentum < -0.3 {
        "Down↑"
    } else {
        "stable"
    };
    debug!(
        conviction = %format!("{:.2}", vol_metrics.conviction),
        momentum = %format!("{:.2} ({})", vol_metrics.momentum, momentum_dir),
        calmness = %format!("{:.2}", vol_metrics.calmness),
        up_fair = %format!("{:.4}", rn_mid),
        up_edge = %format!("{:.4}", up_edge),
        up_cap = ?target_up.map(|v| format!("{:.4}", v)),
        down_fair = %format!("{:.4}", target_down_mid),
        down_edge = %format!("{:.4}", down_edge),
        down_cap = ?target_down.map(|v| format!("{:.4}", v)),
        "Model output"
    );
    
    /* 
     * COMBINED COST VALIDATION
     * Even if each side looks profitable independently, the pair must sum to < max_pair_cost.
     * If combined is too high, scale both caps down proportionally.
     */
    let max_pair_cost_f = max_pair_cost.to_f64().unwrap_or(0.98);
    
    match (target_up, target_down) {
        (Some(up), Some(down)) => {
            let combined = up + down;
            if combined > max_pair_cost_f {
                /* Combined too high - scale down proportionally */
                let scale = max_pair_cost_f / combined;
                let scaled_up = (up * scale).max(tick_size_f);
                let scaled_down = (down * scale).max(tick_size_f);
                
                debug!(
                    original_up = %format!("{:.4}", up),
                    original_down = %format!("{:.4}", down),
                    combined = %format!("{:.4}", combined),
                    scaled_up = %format!("{:.4}", scaled_up),
                    scaled_down = %format!("{:.4}", scaled_down),
                    max = %format!("{:.4}", max_pair_cost_f),
                    "Combined cost too high, scaling caps down"
                );
                
                overrides.up_price_cap = Decimal::from_f64(scaled_up);
                overrides.down_price_cap = Decimal::from_f64(scaled_down);
            } else {
                overrides.up_price_cap = Decimal::from_f64(up);
                overrides.down_price_cap = Decimal::from_f64(down);
            }
        }
        (Some(up), None) => {
            overrides.up_price_cap = Decimal::from_f64(up);
            overrides.down_price_cap = None;
        }
        (None, Some(down)) => {
            overrides.up_price_cap = None;
            overrides.down_price_cap = Decimal::from_f64(down);
        }
        (None, None) => {
            overrides.up_price_cap = None;
            overrides.down_price_cap = None;
        }
    }
    
    /* Pass momentum for asymmetric pricing (chase elusive side) */
    overrides.momentum = vol_metrics.momentum;

    overrides
}

/*
 * Calculate a conservative bid price that will actually get filled.
 * Key principle: we want CONSISTENT fills, not aggressive pricing.
 * Returns None if we shouldn't be buying at current prices (no edge).
 */
fn conservative_bid_price(
    fair_value: f64,
    market_bid: f64,
    market_ask: f64,
    edge_threshold: f64,
    tick: f64,
    calmness: f64,
) -> Option<f64> {
    /* Only buy if fair value is ABOVE market bid - prevents buying overpriced sides */
    let edge_above_bid = fair_value - market_bid;
    
    if edge_above_bid < edge_threshold {
        return None; /* No edge */
    }
    
    let spread = market_ask - market_bid;
    
    /* 
     * Improvement into spread depends on calmness:
     * - Calm: up to 30% into spread
     * - Volatile: stay at or below market bid
     */
    let max_improvement_pct = 0.3 * calmness;
    let max_improvement = spread * max_improvement_pct;
    
    let edge_above_ask = (fair_value - market_ask).max(0.0);
    
    let price = if edge_above_ask >= edge_threshold {
        /* Strong edge: bid up to market_bid + max_improvement, but never above fair_value - tick */
        let aggressive_bid = market_bid + max_improvement;
        aggressive_bid.min(fair_value - tick).max(tick)
    } else {
        /* Moderate edge: bid at market_bid or slightly above */
        let slight_improvement = (max_improvement * 0.5).min(tick);
        (market_bid + slight_improvement).min(fair_value - tick).max(tick)
    };
    
    Some(price)
}

fn apply_imbalance_override(
    overrides: &mut LadderOverrides,
    summary: Option<crate::trend_window::RnJdSummary>,
    up_bid: Decimal,
    down_bid: Decimal,
    imbalance: Decimal,
    max_imbalance: Decimal,
    edge_threshold: Decimal,
) {
    let Some(summary) = summary else {
        return;
    };
    let imbalance_abs = imbalance.abs();
    /* Rebalance when imbalance exceeds threshold (> not >=) */
    if imbalance_abs < max_imbalance {
        return;
    }

    let rn_mid = (summary.rn_bid + summary.rn_ask) / 2.0;
    let up_bid_f = up_bid.to_f64().unwrap_or(0.0);
    let down_bid_f = down_bid.to_f64().unwrap_or(0.0);
    let edge_threshold_f = edge_threshold.to_f64().unwrap_or(0.0025);

    let up_edge = rn_mid - up_bid_f;
    let down_edge = (1.0 - rn_mid) - down_bid_f;

    if imbalance > Decimal::ZERO && down_edge >= edge_threshold_f {
        overrides.allow_imbalance_side = Some(MarketSide::Down);
    } else if imbalance < Decimal::ZERO && up_edge >= edge_threshold_f {
        overrides.allow_imbalance_side = Some(MarketSide::Up);
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TrendStatus {
    NotReady,
    OutOfRange,
    Ready,
}

#[derive(Debug, Clone, Copy)]
struct TrendLogState {
    status: TrendStatus,
    last_log: Instant,
}
