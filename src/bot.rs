use crate::config::ASSETS_BY_NAME;
use crate::config::Config;
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderOverrides, LadderState};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState};
use crate::poller::{InventoryLedger, MarketSide, spawn_order_feed};
use crate::price_feed::{PriceCache, spawn_price_feed};
use crate::training_log::{
    AdaptiveParams, CompletedTrade, TradeAction, TradeRecord, TrainingLogger,
};
use crate::trend_window::TrendWindow;
use chrono::Timelike;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use alloy::primitives::U256;
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
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, B256, Decimal as PolyDecimal};
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;
type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(200);
const BALANCE_REFRESH_SECS: u64 = 30;
/* Maximum combined cost allowed after one leg fills - cancel remaining if exceeded */
const PAIR_PROTECTION_MAX_COST: f64 = 0.995;

const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";
/* Max fair-value haircut (in ticks) applied to the momentum-expensive side. */
const MOMENTUM_FAIR_VALUE_HAIRCUT_TICKS: f64 = 3.0;
/* Side score weights/tuning (all in this file). */
const SCORE_EDGE_WEIGHT: f64 = 1.3;
const SCORE_MOMENTUM_WEIGHT: f64 = 0.5;
const SCORE_VELOCITY_WEIGHT: f64 = 0.6;
const SCORE_VOL_WEIGHT: f64 = 0.7;
const SCORE_IMBALANCE_WEIGHT: f64 = 0.2;
const SCORE_DRIFT_WEIGHT: f64 = 0.3;
const SCORE_EDGE_DENOM: f64 = 0.02;
const SCORE_VELOCITY_DENOM: f64 = 0.01;
const SCORE_DRIFT_DENOM: f64 = 0.01;
const SCORE_VOL_DENOM: f64 = 0.8;
/* Minimum score to allow trading on a side - raised from 0.0 to require meaningful edge */
const SCORE_MIN: f64 = 0.5;
const EXTREME_SKEW_LOW: f64 = 0.05;
const EXTREME_SKEW_HIGH: f64 = 0.95;

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
    ws_client: Arc<AuthenticatedWsClient>,

    ladder_engine: LadderEngine,
    ladder_state: LadderState,
    trend_windows: HashMap<String, TrendWindow>,
    trend_log_state: HashMap<String, TrendLogState>,
    last_missing_price_log: HashMap<String, Instant>,
    last_dry_run_log: HashMap<String, Instant>,
    last_skip_log: HashMap<String, Instant>,
    last_rest_crosscheck_log: HashMap<String, Instant>,
    out_of_range_streak: HashMap<String, usize>,

    /* Training and adaptive learning */
    training_logger: TrainingLogger,
    adaptive_params: AdaptiveParams,
    adaptive_params_path: String,
    pending_orders: HashMap<String, TradeRecord>, /* order_id -> record */
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
            min_order_size: dec!(5),
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

        let adaptive_params_path = AdaptiveParams::default_path().to_string();
        let adaptive_params = AdaptiveParams::load_or_default(&adaptive_params_path);

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            last_order_status_check: HashMap::new(),
            last_model_cancel: HashMap::new(),
            last_balance_refresh: HashMap::new(),
            last_position_log: HashMap::new(),
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
            last_skip_log: HashMap::new(),
            last_rest_crosscheck_log: HashMap::new(),
            out_of_range_streak: HashMap::new(),
            training_logger: TrainingLogger::default(),
            adaptive_params,
            adaptive_params_path,
            pending_orders: HashMap::new(),
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

        self.last_balance_refresh.insert(market_id.to_string(), now);

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

        let up_resp = self.client.balance_allowance(up_req).await?;
        let down_resp = self.client.balance_allowance(down_req).await?;

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

        for (asset, info) in selected {
            let market_id = info.id.clone();
            let market_symbol = market_symbol(&info);
            let market_id_for_ledger = market_id.clone();
            let state = MarketState {
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
                self.last_skip_log.remove(market_id);
                self.last_rest_crosscheck_log.remove(market_id);
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
                        info!(
                            count = cancelled.len(),
                            "Cancelled orders for rolled-over markets"
                        );
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
                    self.ledger
                        .write()
                        .sync_position(market_id.clone(), up_bal, down_bal);
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
        self.config.news_event_times.iter().any(|event| {
            let delta = *event - now;
            delta.num_seconds().abs() <= window.num_seconds()
        })
    }

    async fn scan(&mut self) {
        // Check for pair fill events and cancel remaining legs if combined cost too high
        self.check_pair_fills().await;

        let market_ids: Vec<String> = self.markets.keys().cloned().collect();

        for market_id in market_ids {
            let state = match self.markets.get(&market_id) {
                Some(s) => s,
                None => continue,
            };
            let market_symbol = market_symbol(&state.info);
            let (up_token_id, down_token_id) = (
                state.info.up_token_id.clone(),
                state.info.down_token_id.clone(),
            );

            self.refresh_positions(&market_id, &up_token_id, &down_token_id, &market_symbol)
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

            let (
                up_ask,
                down_ask,
                effective_target,
                mut overrides,
                summary,
                up_bid,
                down_bid,
                mid_start_end,
            ) = match price_snapshot {
                Some((up_bid, up_ask, down_bid, down_ask, up_age, down_age)) => {
                    let now_ms = Utc::now().timestamp_millis();
                    let mut status = TrendStatus::Ready;
                    let mut summary = None;
                    let mut reason = "ok";
                    let mut extra = TrendLogExtra::default();
                    extra.max_age_ms = Some(up_age.max(down_age) as i64);
                    let news_guard_active = self.news_guard_active(Utc::now());

                    // Optional debug-only REST snapshot to compare against WS top-of-book.
                    self.maybe_log_rest_vs_ws(
                        &market_id,
                        &market_symbol,
                        &up_token_id,
                        &down_token_id,
                        up_bid,
                        up_ask,
                        down_bid,
                        down_ask,
                    )
                    .await;

                    let (ranges, span_ms, sample_count, effective_target, overrides, mid_start_end) = {
                        let window = self.trend_windows.entry(market_id.clone()).or_default();
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
                                    if directional_move_dec > self.config.directional_move_threshold
                                    {
                                        status = TrendStatus::OutOfRange;
                                        reason = "jump";
                                    } else if directional_move_dec
                                        <= self.config.high_vol_reversion_threshold
                                    {
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
                        let base_target = self.config.shares_target_per_side;
                        let effective_target = if swing_zone {
                            let scaled = base_target * self.config.swing_zone_target_factor;
                            // Don't scale below the exchange minimum when the user intends to trade.
                            if base_target >= self.ladder_engine.config().min_order_size {
                                scaled.max(self.ladder_engine.config().min_order_size)
                            } else {
                                scaled
                            }
                        } else {
                            base_target
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
                            self.adaptive_params.target_combined_cost_decimal(),
                            self.adaptive_params.min_conviction, /* Learned threshold */
                        );
                        extra.size_multiplier = overrides.size_multiplier;
                        extra.spacing_multiplier = overrides.spacing_multiplier;
                        extra.extra_offset = overrides.extra_offset;
                        extra.up_price_cap = overrides.up_price_cap;
                        extra.down_price_cap = overrides.down_price_cap;
                        extra.max_levels = overrides.max_levels;
                        extra.conviction = summary.map(|s| s.vol_metrics.conviction).unwrap_or(0.0);
                        (ranges, span_ms, sample_count, effective_target, overrides, mid_start_end)
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

                    if status == TrendStatus::OutOfRange {
                        let streak = self
                            .out_of_range_streak
                            .entry(market_id.clone())
                            .or_insert(0);
                        *streak += 1;
                    } else {
                        self.out_of_range_streak.insert(market_id.clone(), 0);
                    }

                    let allow_model_cancel = matches!(status, TrendStatus::OutOfRange)
                        && self
                            .out_of_range_streak
                            .get(&market_id)
                            .copied()
                            .unwrap_or(0)
                            >= 2;

                    if status != TrendStatus::Ready {
                        let open_orders = self.ledger.read().open_orders_for_market(&market_id);
                        if !open_orders.is_empty()
                            && allow_model_cancel
                            && self.should_model_cancel(&market_id)
                        {
                            /*
                             * SMART CANCELLATION: Only cancel orders on the side losing edge.
                             * - momentum > 0: Up getting expensive → cancel Up orders (keep Down)
                             * - momentum < 0: Down getting expensive → cancel Down orders (keep Up)
                             * - momentum ~0: cancel all (no clear direction)
                             */
                            let momentum = summary.map(|s| s.vol_metrics.momentum).unwrap_or(0.0);
                            let cancel_ids: Vec<String> = open_orders
                                .into_iter()
                                .filter(|o| {
                                    if momentum > 0.2 {
                                        /* Up expensive → only cancel Up orders */
                                        o.side == MarketSide::Up
                                    } else if momentum < -0.2 {
                                        /* Down expensive → only cancel Down orders */
                                        o.side == MarketSide::Down
                                    } else {
                                        /* Unclear → cancel all */
                                        true
                                    }
                                })
                                .map(|o| o.order_id)
                                .collect();

                            if !cancel_ids.is_empty() {
                                let side_str = if momentum > 0.2 {
                                    "Up"
                                } else if momentum < -0.2 {
                                    "Down"
                                } else {
                                    "all"
                                };
                                info!(
                                    market = %market_symbol,
                                    reason = %reason,
                                    side = %side_str,
                                    count = cancel_ids.len(),
                                    "Model pause: cancelling orders on expensive side"
                                );
                                match self.cancel_orders(&cancel_ids).await {
                                    Ok(cancelled) => {
                                        self.ledger.write().mark_orders_cancelled(&cancelled);
                                    }
                                    Err(e) => {
                                        error!(
                                            market = %market_symbol,
                                            error = %e,
                                            "Failed to cancel orders on model pause"
                                        );
                                    }
                                }
                            }
                        }

                        if self.should_log_skip(&market_id) {
                            debug!(
                                market = %market_symbol,
                                status = ?status,
                                reason = %reason,
                                up_bid = %up_bid,
                                up_ask = %up_ask,
                                down_bid = %down_bid,
                                down_ask = %down_ask,
                                max_age_ms = ?extra.max_age_ms,
                                "Skipping ladder: trend gate"
                            );
                        }
                        continue;
                    }

                    (
                        up_ask,
                        down_ask,
                        effective_target,
                        overrides,
                        summary,
                        up_bid,
                        down_bid,
                        mid_start_end,
                    )
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
                if self.should_log_skip(&market_id) {
                    debug!(
                        market = %market_symbol,
                        up_ask = %up_ask,
                        down_ask = %down_ask,
                        "Skipping ladder: no reladder + cooldown"
                    );
                }
                continue;
            }

            let mid_up = (up_bid + up_ask) / dec!(2);
            let mid_up_f64 = mid_up.to_f64().unwrap_or(0.5);
            if !self.config.allow_extreme_skew
                && (mid_up_f64 <= EXTREME_SKEW_LOW || mid_up_f64 >= EXTREME_SKEW_HIGH)
            {
                info!(
                    market = %market_symbol,
                    mid = %format!("{:.4}", mid_up_f64),
                    "Extreme skew; skipping bids (enable --allow-extreme-skew to override)"
                );
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

            // Mirror ladder "room" math for diagnostics (why no orders were generated).
            let min_order_size = self.ladder_engine.config().min_order_size;
            let up_room = if pos.up_shares >= effective_target {
                Decimal::ZERO
            } else {
                (effective_target - total_up).max(Decimal::ZERO)
            };
            let down_room = if pos.down_shares >= effective_target {
                Decimal::ZERO
            } else {
                (effective_target - total_down).max(Decimal::ZERO)
            };

            apply_imbalance_override(
                &mut overrides,
                summary,
                up_bid,
                down_bid,
                imbalance,
                self.ladder_engine.config().max_imbalance,
                self.config.edge_threshold,
            );

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
                debug!(
                    market = %market_symbol,
                    "position up={:.4} down={:.4} pending(up={:.4} down={:.4}) total(up={:.4} down={:.4}){} target={:.2}",
                    pos.up_shares,
                    pos.down_shares,
                    pending_up,
                    pending_down,
                    total_up,
                    total_down,
                    imbalance_str,
                    effective_target
                );
            }

            let plan = self.ladder_engine.compute_ladder_with_target(
                up_ask,
                down_ask,
                &pos,
                &open_orders,
                Some(effective_target),
                Some(overrides),
            );

            if plan.cancellations.is_empty() && plan.orders.is_empty() {
                if self.should_log_skip(&market_id) {
                    debug!(
                        market = %market_symbol,
                        up_bid = %up_bid,
                        up_ask = %up_ask,
                        down_bid = %down_bid,
                        down_ask = %down_ask,
                        combined_ask = %(up_ask + down_ask),
                        up_room = %up_room,
                        down_room = %down_room,
                        min_order_size = %min_order_size,
                        target = %effective_target,
                        total_up = %total_up,
                        total_down = %total_down,
                        pending_up = %pending_up,
                        pending_down = %pending_down,
                        "No ladder updates (no cancels, no orders)"
                    );
                }
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

            if plan.orders.is_empty() {
                if self.should_log_skip(&market_id) {
                    debug!(
                        market = %market_symbol,
                        cancelled = plan.cancellations.len(),
                        up_room = %up_room,
                        down_room = %down_room,
                        min_order_size = %min_order_size,
                        target = %effective_target,
                        "No new orders in ladder plan"
                    );
                }
            }

            if !plan.orders.is_empty() {
                let scores = compute_side_scores(
                    summary,
                    up_bid,
                    down_bid,
                    imbalance,
                    self.ladder_engine.config().max_imbalance,
                    mid_start_end,
                    self.config.trend_window_secs,
                );
                
                /*
                 * Score-based filtering with imbalance override:
                 * - If we're imbalanced, ALWAYS allow the underweight side
                 * - This ensures we always have limit orders in the book for both sides
                 * - The limit order will fill when the market comes to us
                 */
                let max_imbalance = self.ladder_engine.config().max_imbalance;
                let imbalance_favors_up = imbalance < -max_imbalance * dec!(0.5);  // Heavy on Down, need Up
                let imbalance_favors_down = imbalance > max_imbalance * dec!(0.5); // Heavy on Up, need Down
                
                let allow_up = scores.up >= SCORE_MIN || imbalance_favors_up;
                let allow_down = scores.down >= SCORE_MIN || imbalance_favors_down;
                
                if imbalance_favors_up && scores.up < SCORE_MIN {
                    debug!(
                        market = %market_symbol,
                        score_up = %format!("{:.2}", scores.up),
                        imbalance = %imbalance,
                        "Allowing Up order despite low score - need to rebalance"
                    );
                }
                if imbalance_favors_down && scores.down < SCORE_MIN {
                    debug!(
                        market = %market_symbol,
                        score_down = %format!("{:.2}", scores.down),
                        imbalance = %imbalance,
                        "Allowing Down order despite low score - need to rebalance"
                    );
                }
                
                let mut orders = plan.orders;

                /*
                 * HYBRID MODE (paired-preserving):
                 * - If the ladder engine produced a paired plan (Up + Down), NEVER drop a leg.
                 * - Use model score/conviction only to scale depth (number of pairs), not to
                 *   filter out one side entirely.
                 */
                let has_up = orders.iter().any(|o| o.side == MarketSide::Up);
                let has_down = orders.iter().any(|o| o.side == MarketSide::Down);
                let is_paired_plan = has_up && has_down;

                if is_paired_plan {
                    let available_pairs = (orders.len() / 2).max(1);
                    let conviction = summary.map(|s| s.vol_metrics.conviction).unwrap_or(0.0);
                    let pair_score = scores.up.min(scores.down);
                    let max_imbalance = self.ladder_engine.config().max_imbalance;
                    let imbalance_abs = imbalance.abs();

                    let keep_pairs = if pair_score >= SCORE_MIN {
                        available_pairs
                    } else if conviction >= 0.65 || pair_score >= SCORE_MIN * 0.5 {
                        available_pairs.min(2)
                    } else {
                        1
                    };
                    let mut keep_pairs = keep_pairs.clamp(1, available_pairs);

                    // If we're already materially imbalanced, keep only a minimal paired quote
                    // and let inventory-skewed pricing + rebalancing do the work.
                    if max_imbalance > Decimal::ZERO {
                        if imbalance_abs >= max_imbalance {
                            keep_pairs = 1;
                        } else if imbalance_abs >= max_imbalance * dec!(0.5) {
                            keep_pairs = keep_pairs.min(2);
                        }
                    }

                    if keep_pairs < available_pairs {
                        debug!(
                            market = %market_symbol,
                            keep_pairs,
                            available_pairs,
                            imbalance = %imbalance,
                            max_imbalance = %max_imbalance,
                            conviction = %format!("{:.2}", conviction),
                            score_up = %format!("{:.2}", scores.up),
                            score_down = %format!("{:.2}", scores.down),
                            "Paired plan: limiting depth (keeping both legs)"
                        );
                        orders.truncate(keep_pairs * 2);
                    }
                } else {
                    // Single-side plans can still be score-gated.
                    orders.retain(|order| match order.side {
                        MarketSide::Up => allow_up,
                        MarketSide::Down => allow_down,
                    });
                }
                if orders.is_empty() {
                    if self.should_log_skip(&market_id) {
                        debug!(
                            market = %market_symbol,
                            is_paired_plan,
                            allow_up,
                            allow_down,
                            score_up = %format!("{:.2}", scores.up),
                            score_down = %format!("{:.2}", scores.down),
                            "All planned orders were filtered out"
                        );
                    }
                    continue;
                }

                if self.should_log_skip(&market_id) {
                    let top_up = orders
                        .iter()
                        .filter(|o| o.side == MarketSide::Up)
                        .map(|o| o.price)
                        .max();
                    let top_down = orders
                        .iter()
                        .filter(|o| o.side == MarketSide::Down)
                        .map(|o| o.price)
                        .max();
                    let top_pair = top_up.zip(top_down).map(|(u, d)| u + d);

                    debug!(
                        market = %market_symbol,
                        up_bid = %up_bid,
                        up_ask = %up_ask,
                        down_bid = %down_bid,
                        down_ask = %down_ask,
                        combined_ask = %(up_ask + down_ask),
                        top_up = ?top_up,
                        top_down = ?top_down,
                        top_pair = ?top_pair,
                        pair_cap = %self.ladder_engine.config().max_pair_cost,
                        up_room = %up_room,
                        down_room = %down_room,
                        min_order_size = %min_order_size,
                        target = %effective_target,
                        "WS quote vs intended ladder (summary)"
                    );
                }

                for order in &orders {
                    let (side_cap, rn_bid, rn_ask, momentum, conviction) = match summary {
                        Some(s) => (
                            match order.side {
                                MarketSide::Up => overrides.up_price_cap,
                                MarketSide::Down => overrides.down_price_cap,
                            },
                            s.rn_bid,
                            s.rn_ask,
                            s.vol_metrics.momentum,
                            s.vol_metrics.conviction,
                        ),
                        None => (None, 0.0, 0.0, 0.0, 0.0),
                    };
                    let (side_bid, side_ask) = match order.side {
                        MarketSide::Up => (up_bid, up_ask),
                        MarketSide::Down => (down_bid, down_ask),
                    };
                    info!(
                        market = %market_symbol,
                        side = ?order.side,
                        price = %order.price,
                        size = %order.size,
                        market_bid = %side_bid,
                        market_ask = %side_ask,
                        cap = ?side_cap.map(format_decimal),
                        rn_bid = %format!("{:.4}", rn_bid),
                        rn_ask = %format!("{:.4}", rn_ask),
                        momentum = %format!("{:.2}", momentum),
                        conviction = %format!("{:.2}", conviction),
                        score_up = %format!("{:.2}", scores.up),
                        score_down = %format!("{:.2}", scores.down),
                        edge_up = %format!("{:.4}", scores.edge_up),
                        edge_down = %format!("{:.4}", scores.edge_down),
                        edge_up_score = %format!("{:.2}", scores.edge_up_score),
                        edge_down_score = %format!("{:.2}", scores.edge_down_score),
                        velocity = %format!("{:.4}", scores.velocity),
                        drift = %format!("{:.4}", scores.drift),
                        vol_penalty = %format!("{:.2}", scores.vol_penalty),
                        imbalance_bias = %format!("{:.2}", scores.imbalance_bias),
                        conviction_scale = %format!("{:.2}", scores.conviction_scale),
                        "Buy decision"
                    );
                }
                if self.config.dry_run {
                    let should_log = self.should_log_dry_run(&market_id);
                    if should_log {
                        info!(
                            market = %market_symbol,
                            count = orders.len(),
                            "Dry run: skipping order placement"
                        );
                        for order in &orders {
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
                    .build_signed_orders(&orders, &up_token_id, &down_token_id)
                    .await;

                if !signed_orders.is_empty() {
                    match self.client.post_orders(signed_orders).await {
                        Ok(responses) => {
                            let mut placed = 0;
                            let mut placed_order_ids: Vec<(String, MarketSide)> = Vec::new();
                            
                            for (resp, order) in responses.iter().zip(orders.iter()) {
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
                                    placed_order_ids.push((resp.order_id.clone(), order.side));

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

                            // Link paired orders for pair execution protection
                            // Find Up and Down orders and link them together
                            let up_orders: Vec<_> = placed_order_ids.iter()
                                .filter(|(_, side)| *side == MarketSide::Up)
                                .map(|(id, _)| id.clone())
                                .collect();
                            let down_orders: Vec<_> = placed_order_ids.iter()
                                .filter(|(_, side)| *side == MarketSide::Down)
                                .map(|(id, _)| id.clone())
                                .collect();
                            
                            // Link matching pairs (by index)
                            {
                                let mut ledger = self.ledger.write();
                                for (up_id, down_id) in up_orders.iter().zip(down_orders.iter()) {
                                    ledger.link_paired_orders(up_id, down_id);
                                    debug!(
                                        up_order = %up_id,
                                        down_order = %down_id,
                                        "Linked paired orders for protection"
                                    );
                                }
                            }

                            info!(
                                market = %market_symbol,
                                up_ask = %up_ask,
                                down_ask = %down_ask,
                                cancelled = plan.cancellations.len(),
                                placed,
                                pairs_linked = up_orders.len().min(down_orders.len()),
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
     * Now uses actual fill prices from the ledger (via WebSocket updates)
     * instead of placed prices for accurate P&L calculation.
     */
    fn check_completed_pairs(&mut self, market_id: &str) {
        let ledger = self.ledger.read();
        let (up_shares, down_shares) = ledger.confirmed_position(market_id);
        
        /*
         * Calculate average fill prices from actual fills tracked in the ledger.
         * NOTE: use filled_size, not the placed size from the training record.
         */
        let mut up_fill_value = 0.0;
        let mut down_fill_value = 0.0;
        let mut up_fill_size = 0.0;
        let mut down_fill_size = 0.0;

        /* Placement-time features (for training) */
        let mut placement_conviction_sum = 0.0;
        let mut placement_momentum_sum = 0.0;
        let mut placement_calmness_sum = 0.0;
        let mut placement_n = 0.0;

        for (order_id, record) in self
            .pending_orders
            .iter()
            .filter(|(_, r)| r.market_id == market_id)
        {
            placement_conviction_sum += record.conviction;
            placement_momentum_sum += record.momentum;
            placement_calmness_sum += record.calmness;
            placement_n += 1.0;

            let filled = ledger.filled_size(order_id).to_f64().unwrap_or(0.0);
            if filled <= 0.0 {
                continue;
            }

            let limit_price = ledger
                .order_price(order_id)
                .and_then(|d| d.to_f64())
                .or(record.order_price);
            let limit_price = limit_price.unwrap_or(0.5);

            match record.order_side.as_deref() {
                Some("Up") => {
                    up_fill_value += limit_price * filled;
                    up_fill_size += filled;
                }
                Some("Down") => {
                    down_fill_value += limit_price * filled;
                    down_fill_size += filled;
                }
                _ => {}
            }
        }
        drop(ledger);

        /* Check if we have a complete pair (at least min_order_size of each side) */
        let min_pair_size = up_shares.min(down_shares);
        let min_pair_threshold = self.ladder_engine.config().min_order_size;
        if min_pair_size >= min_pair_threshold && up_fill_size > 0.0 && down_fill_size > 0.0 {
            /* Calculate profit: 1 share pair = $1 payout, cost = what we paid */
            let avg_up_cost = up_fill_value / up_fill_size;
            let avg_down_cost = down_fill_value / down_fill_size;

            let combined_cost = avg_up_cost + avg_down_cost;
            let profit_per_share = 1.0 - combined_cost;
            let profit_cents = profit_per_share * 100.0;

            /* Use placement-time features when available; fall back to current model state. */
            let summary = self
                .trend_windows
                .get_mut(market_id)
                .and_then(|w| w.rn_jd_summary());
            let (conviction, momentum, calmness) = if placement_n > 0.0 {
                (
                    placement_conviction_sum / placement_n,
                    placement_momentum_sum / placement_n,
                    placement_calmness_sum / placement_n,
                )
            } else {
                summary
                    .map(|s| {
                        (
                            s.vol_metrics.conviction,
                            s.vol_metrics.momentum,
                            s.vol_metrics.calmness,
                        )
                    })
                    .unwrap_or((0.5, 0.0, 0.5))
            };
            let mid_start_end = self
                .trend_windows
                .get(market_id)
                .and_then(|w| w.mid_start_end());
            let imbalance = up_shares - down_shares;
            let scores = compute_side_scores(
                summary,
                Decimal::from_f64(avg_up_cost).unwrap_or_default(),
                Decimal::from_f64(avg_down_cost).unwrap_or_default(),
                imbalance,
                self.ladder_engine.config().max_imbalance,
                mid_start_end,
                self.config.trend_window_secs,
            );

            /* Record completed trade for adaptive learning */
            let trade = CompletedTrade {
                conviction_at_trade: conviction,
                momentum_at_trade: momentum,
                calmness_at_trade: calmness,
                combined_cost,
                profit_cents,
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                score_up: scores.up,
                score_down: scores.down,
                edge_up: scores.edge_up,
                edge_down: scores.edge_down,
                velocity: scores.velocity,
                drift: scores.drift,
                vol_penalty: scores.vol_penalty,
                imbalance_bias: scores.imbalance_bias,
                conviction_scale: scores.conviction_scale,
            };

            self.training_logger.log_completed_trade(&trade);
            self.adaptive_params.record_trade(trade);
            self.adaptive_params.save(&self.adaptive_params_path);

            /* Clear pending orders for this market to avoid double-counting */
            self.pending_orders.retain(|_, r| r.market_id != market_id);
        }
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
                self.last_dry_run_log.insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_skip(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(3);
        match self.last_skip_log.get(market_id) {
            Some(last) if now.duration_since(*last) < min_interval => false,
            _ => {
                self.last_skip_log.insert(market_id.to_string(), now);
                true
            }
        }
    }

    fn should_log_rest_crosscheck(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(10);
        match self.last_rest_crosscheck_log.get(market_id) {
            Some(last) if now.duration_since(*last) < min_interval => false,
            _ => {
                self.last_rest_crosscheck_log
                    .insert(market_id.to_string(), now);
                true
            }
        }
    }

    async fn maybe_log_rest_vs_ws(
        &mut self,
        market_id: &str,
        market_symbol: &str,
        up_token_id: &str,
        down_token_id: &str,
        ws_up_bid: Decimal,
        ws_up_ask: Decimal,
        ws_down_bid: Decimal,
        ws_down_ask: Decimal,
    ) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }
        if !self.should_log_rest_crosscheck(market_id) {
            return;
        }

        let up_token = match U256::from_str(up_token_id) {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    market = %market_symbol,
                    token_id = %up_token_id,
                    error = %e,
                    "REST crosscheck: invalid Up token id"
                );
                return;
            }
        };
        let down_token = match U256::from_str(down_token_id) {
            Ok(id) => id,
            Err(e) => {
                warn!(
                    market = %market_symbol,
                    token_id = %down_token_id,
                    error = %e,
                    "REST crosscheck: invalid Down token id"
                );
                return;
            }
        };

        let up_req = OrderBookSummaryRequest::builder().token_id(up_token).build();
        let down_req = OrderBookSummaryRequest::builder().token_id(down_token).build();
        let requests = vec![up_req, down_req];

        let books = match self.client.order_books(&requests).await {
            Ok(b) => b,
            Err(e) => {
                warn!(market = %market_symbol, error = %e, "REST crosscheck: order_books failed");
                return;
            }
        };

        let mut rest_up = None;
        let mut rest_down = None;
        for book in books {
            if book.asset_id == up_token {
                rest_up = Some(book);
            } else if book.asset_id == down_token {
                rest_down = Some(book);
            }
        }

        let Some(rest_up) = rest_up else {
            warn!(market = %market_symbol, "REST crosscheck: missing Up book in response");
            return;
        };
        let Some(rest_down) = rest_down else {
            warn!(market = %market_symbol, "REST crosscheck: missing Down book in response");
            return;
        };

        let rest_up_bid = rest_up.bids.iter().map(|l| l.price).max();
        let rest_up_ask = rest_up.asks.iter().map(|l| l.price).min();
        let rest_down_bid = rest_down.bids.iter().map(|l| l.price).max();
        let rest_down_ask = rest_down.asks.iter().map(|l| l.price).min();

        debug!(
            market = %market_symbol,
            ws_up_bid = %ws_up_bid,
            ws_up_ask = %ws_up_ask,
            ws_down_bid = %ws_down_bid,
            ws_down_ask = %ws_down_ask,
            rest_up_bid = ?rest_up_bid,
            rest_up_ask = ?rest_up_ask,
            rest_down_bid = ?rest_down_bid,
            rest_down_ask = ?rest_down_ask,
            rest_up_ts = %rest_up.timestamp,
            rest_down_ts = %rest_down.timestamp,
            rest_min_size = %rest_up.min_order_size,
            rest_tick = ?rest_up.tick_size,
            "REST vs WS top-of-book crosscheck"
        );
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
                self.last_model_cancel.insert(market_id.to_string(), now);
                true
            }
        }
    }

    /// Check for pair fill events and (optionally) protect against bad pair completion.
    ///
    /// Important: for this bot's maker-pair strategy, we generally want the remaining
    /// resting order to stay on the book even if the current ask has moved away.
    /// Otherwise we end up with unhedged single-leg exposure.
    async fn check_pair_fills(&mut self) {
        let pair_fills = self.ledger.write().take_pending_pair_fills();
        
        if pair_fills.is_empty() {
            return;
        }

        for event in pair_fills {
            // Get current market ask for the remaining side (for diagnostics only)
            let (remaining_ask, market_symbol, remaining_token_id) = {
                let state = match self.markets.get(&event.market_id) {
                    Some(s) => s,
                    None => continue,
                };
                let symbol = market_symbol(&state.info);
                
                let cache = self.price_cache.read();
                let now_ms = Utc::now().timestamp_millis() as u64;
                
                // The remaining side is opposite of the filled side
                let remaining_token = match event.filled_side {
                    MarketSide::Up => &state.info.down_token_id,
                    MarketSide::Down => &state.info.up_token_id,
                };
                
                match cache.get_with_age(remaining_token, now_ms) {
                    Some((_bid, ask, _age)) => (ask, symbol, remaining_token.clone()),
                    None => continue, // No price data, can't evaluate
                }
            };

            let remaining_side = match event.filled_side {
                MarketSide::Up => MarketSide::Down,
                MarketSide::Down => MarketSide::Up,
            };

            // Our *resting* paired order price is what matters for arb (not the current ask).
            let paired_limit_price = {
                let ledger = self.ledger.read();
                ledger.get_fill_price(&event.paired_order_id)
            };

            let filled_price_f = event.filled_price.to_f64().unwrap_or(0.5);
            let remaining_ask_f = remaining_ask.to_f64().unwrap_or(0.5);
            let projected_combined_at_ask = filled_price_f + remaining_ask_f;

            let max_pair_cost = self.ladder_engine.config().max_pair_cost;
            let max_pair_cost_f = max_pair_cost.to_f64().unwrap_or(PAIR_PROTECTION_MAX_COST);

            if let Some(paired_price) = paired_limit_price {
                let paired_price_f = paired_price.to_f64().unwrap_or(0.5);
                let projected_combined_at_limit = filled_price_f + paired_price_f;

                // Only cancel if our remaining LIMIT would make the pair exceed the cap.
                // (For paired ladder orders this should basically never happen, but keep the guard.)
                if projected_combined_at_limit > max_pair_cost_f {
                    warn!(
                        market = %market_symbol,
                        filled_side = ?event.filled_side,
                        filled_price = %format!("{:.4}", filled_price_f),
                        remaining_side = ?remaining_side,
                        paired_limit = %format!("{:.4}", paired_price_f),
                        combined_at_limit = %format!("{:.4}", projected_combined_at_limit),
                        max_pair_cost = %format!("{:.4}", max_pair_cost_f),
                        remaining_ask = %format!("{:.4}", remaining_ask_f),
                        combined_at_ask = %format!("{:.4}", projected_combined_at_ask),
                        "Pair protection: cancelling remaining leg (limit price exceeds pair cap)"
                    );

                    if !self.config.dry_run {
                        match self.cancel_orders(&[event.paired_order_id.clone()]).await {
                            Ok(cancelled) => {
                                if !cancelled.is_empty() {
                                    self.ledger.write().mark_orders_cancelled(&cancelled);
                                }
                            }
                            Err(e) => {
                                error!(
                                    market = %market_symbol,
                                    error = %e,
                                    "Failed to cancel remaining leg of bad pair"
                                );
                            }
                        }
                    }
                } else {
                    debug!(
                        market = %market_symbol,
                        filled_side = ?event.filled_side,
                        filled_price = %format!("{:.4}", filled_price_f),
                        remaining_side = ?remaining_side,
                        paired_limit = %format!("{:.4}", paired_price_f),
                        combined_at_limit = %format!("{:.4}", projected_combined_at_limit),
                        remaining_ask = %format!("{:.4}", remaining_ask_f),
                        combined_at_ask = %format!("{:.4}", projected_combined_at_ask),
                        remaining_token = %remaining_token_id,
                        "Pair fill: keeping remaining resting leg"
                    );
                }
            } else {
                // If we don't know the paired price, fall back to old behavior (ask-based)
                // but keep it conservative: do NOT cancel automatically; just log.
                debug!(
                    market = %market_symbol,
                    filled_side = ?event.filled_side,
                    filled_price = %format!("{:.4}", filled_price_f),
                    remaining_ask = %format!("{:.4}", remaining_ask_f),
                    combined_at_ask = %format!("{:.4}", projected_combined_at_ask),
                    "Pair fill: missing paired order info; skipping protection"
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

                        self.last_order_status_check.insert(order_id.clone(), now);

                        match self.client.order(order_id).await {
                            Ok(order) => {
                                let is_live = matches!(order.status, OrderStatusType::Live);
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
                        self.ledger.write().mark_orders_cancelled(&confirmed_closed);
                    }
                }

                // Prune training records for cancelled orders that never filled.
                if !response.canceled.is_empty() {
                    let ledger = self.ledger.read();
                    for order_id in &response.canceled {
                        if ledger.filled_size(order_id).is_zero() {
                            self.pending_orders.remove(order_id);
                        }
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
    let up_cap = extra
        .up_price_cap
        .map(format_decimal)
        .unwrap_or_else(|| "-".to_string());
    let down_cap = extra
        .down_price_cap
        .map(format_decimal)
        .unwrap_or_else(|| "-".to_string());

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
    min_conviction: f64, /* Adaptive: learned from performance */
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

    overrides.spacing_multiplier =
        Decimal::from_f64(base_mult.clamp(0.8, 2.5)).unwrap_or(Decimal::ONE);

    if overrides.spacing_multiplier > Decimal::ONE {
        let extra = tick_size * (overrides.spacing_multiplier - Decimal::ONE);
        overrides.extra_offset = extra;
    }

    let rn_mid = (summary.rn_bid + summary.rn_ask) / 2.0;
    let rn_bid = summary.rn_bid;
    let rn_ask = summary.rn_ask;
    let up_bid_f = up_bid.to_f64().unwrap_or(0.0);
    let up_ask_f = up_ask.to_f64().unwrap_or(1.0);
    let edge_threshold_f = edge_threshold.to_f64().unwrap_or(0.0025);
    let tick_size_f = tick_size.to_f64().unwrap_or(0.01);
    let momentum = vol_metrics.momentum.clamp(-1.0, 1.0);
    let fair_value_haircut = tick_size_f * MOMENTUM_FAIR_VALUE_HAIRCUT_TICKS;
    let up_fair_base = rn_bid;
    let down_fair_base = 1.0 - rn_ask;
    let up_fair = (up_fair_base - fair_value_haircut * momentum.max(0.0))
        .clamp(tick_size_f, 1.0 - tick_size_f);
    let down_fair = (down_fair_base - fair_value_haircut * (-momentum).max(0.0))
        .clamp(tick_size_f, 1.0 - tick_size_f);

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
    overrides.size_multiplier =
        size_scale_min + size_range * Decimal::from_f64(size_scale).unwrap_or(Decimal::ONE);

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
        up_fair,
        up_bid_f,
        up_ask_f,
        edge_threshold_f,
        tick_size_f,
        vol_metrics.calmness,
    );

    let target_down = conservative_bid_price(
        down_fair,
        down_bid_f,
        down_ask_f,
        edge_threshold_f,
        tick_size_f,
        vol_metrics.calmness,
    );

    /* Log edge calculations for debugging */
    let up_edge = up_fair - up_bid_f;
    let down_edge = down_fair - down_bid_f;
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
        up_fair_raw = %format!("{:.4}", rn_mid),
        up_rn_bid = %format!("{:.4}", rn_bid),
        up_rn_ask = %format!("{:.4}", rn_ask),
        up_fair = %format!("{:.4}", up_fair),
        up_edge = %format!("{:.4}", up_edge),
        up_cap = ?target_up.map(|v| format!("{:.4}", v)),
        down_fair = %format!("{:.4}", down_fair),
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
        (market_bid + slight_improvement)
            .min(fair_value - tick)
            .max(tick)
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

#[derive(Debug, Clone, Copy, Default)]
struct SideScoreBreakdown {
    up: f64,
    down: f64,
    edge_up: f64,
    edge_down: f64,
    edge_up_score: f64,
    edge_down_score: f64,
    velocity: f64,
    drift: f64,
    vol_penalty: f64,
    imbalance_bias: f64,
    conviction_scale: f64,
}

fn compute_side_scores(
    summary: Option<crate::trend_window::RnJdSummary>,
    up_bid: Decimal,
    down_bid: Decimal,
    imbalance: Decimal,
    max_imbalance: Decimal,
    mid_start_end: Option<(f64, f64)>,
    window_secs: u64,
) -> SideScoreBreakdown {
    let Some(summary) = summary else {
        return SideScoreBreakdown::default();
    };

    let up_bid_f = up_bid.to_f64().unwrap_or(0.0);
    let down_bid_f = down_bid.to_f64().unwrap_or(0.0);
    let up_fair = summary.rn_bid;
    let down_fair = 1.0 - summary.rn_ask;

    let edge_up = (up_fair - up_bid_f).max(-1.0);
    let edge_down = (down_fair - down_bid_f).max(-1.0);
    let edge_up_score = (edge_up / SCORE_EDGE_DENOM).clamp(-2.0, 2.0);
    let edge_down_score = (edge_down / SCORE_EDGE_DENOM).clamp(-2.0, 2.0);

    let momentum = summary.vol_metrics.momentum.clamp(-1.0, 1.0);
    let momentum_up = momentum;
    let momentum_down = -momentum;

    let velocity = match mid_start_end {
        Some((start, end)) if window_secs > 0 => (end - start) / (window_secs as f64),
        _ => 0.0,
    };
    let velocity_up = (velocity / SCORE_VELOCITY_DENOM).clamp(-2.0, 2.0);
    let velocity_down = (-velocity / SCORE_VELOCITY_DENOM).clamp(-2.0, 2.0);

    let drift = (summary.drift / SCORE_DRIFT_DENOM).clamp(-2.0, 2.0);
    let drift_up = drift;
    let drift_down = -drift;

    let vol_penalty = (summary.vol_metrics.realized_vol / SCORE_VOL_DENOM).clamp(0.0, 2.0);
    let imbalance_bias = if max_imbalance > Decimal::ZERO {
        (imbalance / max_imbalance)
            .to_f64()
            .unwrap_or(0.0)
            .clamp(-1.0, 1.0)
    } else {
        0.0
    };

    let conviction_scale = 0.5 + 0.5 * summary.vol_metrics.conviction;

    let up_score = (SCORE_EDGE_WEIGHT * edge_up_score
        + SCORE_MOMENTUM_WEIGHT * momentum_up
        + SCORE_VELOCITY_WEIGHT * velocity_up
        + SCORE_DRIFT_WEIGHT * drift_up
        - SCORE_VOL_WEIGHT * vol_penalty
        - SCORE_IMBALANCE_WEIGHT * imbalance_bias)
        * conviction_scale;

    let down_score = (SCORE_EDGE_WEIGHT * edge_down_score
        + SCORE_MOMENTUM_WEIGHT * momentum_down
        + SCORE_VELOCITY_WEIGHT * velocity_down
        + SCORE_DRIFT_WEIGHT * drift_down
        - SCORE_VOL_WEIGHT * vol_penalty
        + SCORE_IMBALANCE_WEIGHT * imbalance_bias)
        * conviction_scale;

    SideScoreBreakdown {
        up: up_score,
        down: down_score,
        edge_up,
        edge_down,
        edge_up_score,
        edge_down_score,
        velocity,
        drift,
        vol_penalty,
        imbalance_bias,
        conviction_scale,
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
