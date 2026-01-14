use crate::config::Config;
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderOverrides, LadderState};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};
use crate::poller::{InventoryLedger, MarketSide, spawn_order_poller};
use crate::price_feed::{PriceCache, spawn_price_feed};
use crate::trend_window::TrendWindow;
use crate::config::ASSETS_BY_NAME;
use chrono::Timelike;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{AssetType, Side as ClobSide, SignatureType, SignedOrder};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal as PolyDecimal};
use rust_decimal_macros::dec;
use alloy::primitives::U256;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(200);

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

    ledger: Arc<RwLock<InventoryLedger>>,
    price_cache: Arc<RwLock<PriceCache>>,
    _order_poller: tokio::task::JoinHandle<()>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    active_market_ids: Arc<RwLock<Vec<String>>>,
    active_token_pairs: Arc<RwLock<Vec<(String, String)>>>,

    ladder_engine: LadderEngine,
    ladder_state: LadderState,
    trend_windows: HashMap<String, TrendWindow>,
    trend_log_state: HashMap<String, TrendLogState>,
    last_missing_price_log: HashMap<String, Instant>,
    last_dry_run_log: HashMap<String, Instant>,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let price_cache = Arc::new(RwLock::new(PriceCache::default()));

        let mut ladder_config = LadderConfig::default();
        ladder_config.size_per_level = config.order_size;
        ladder_config.target_per_side = config.shares_target_per_side;
        ladder_config.max_pair_cost = config.target_total_cost;
        ladder_config.top_offset = config.maker_price_offset;
        let ladder_engine = LadderEngine::new(ladder_config);
        let ladder_state = LadderState::default();

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));

        let addr = Address::from_str(config.polymarket_proxy_address.trim())
            .map_err(|e| anyhow::anyhow!("Invalid POLYMARKET_PROXY_ADDRESS: {}", e))?;

        let client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer_with_chain)
            .funder(addr)
            .signature_type(SignatureType::Proxy)
            .authenticate()
            .await?;

        let client = Arc::new(client);

        let ledger = Arc::new(RwLock::new(InventoryLedger::default()));
        let active_market_ids = Arc::new(RwLock::new(Vec::new()));
        let active_token_pairs = Arc::new(RwLock::new(Vec::new()));

        let poller = spawn_order_poller(
            client.clone(),
            ledger.clone(),
            active_token_pairs.clone(),
            Duration::from_millis(3000),
        );

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            active_token_pairs,
            active_market_ids,
            ledger,
            _order_poller: poller,
            price_cache,
            _price_feed: None,
            ladder_engine,
            ladder_state,
            trend_windows: HashMap::new(),
            trend_log_state: HashMap::new(),
            last_missing_price_log: HashMap::new(),
            last_dry_run_log: HashMap::new(),
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
            self.ledger.write().register_market(
                market_id_for_ledger,
                info.up_token_id.clone(),
                info.down_token_id.clone(),
                market_symbol,
            );
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
            for (market_id, up_token_id, down_token_id) in removed_markets {
                ledger.unregister_market(&market_id, &up_token_id, &down_token_id);
            }
        }

        // Update active market IDs after processing all markets
        {
            let mut ids = self.active_market_ids.write();
            *ids = self.markets.keys().cloned().collect();
        }

        {
            let mut pairs = self.active_token_pairs.write();
            *pairs = self
                .markets
                .values()
                .map(|state| {
                    (
                        state.info.up_token_id.clone(),
                        state.info.down_token_id.clone(),
                    )
                })
                .collect();
        }

        let token_ids: Vec<String> = self
            .markets
            .values()
            .flat_map(|m| vec![m.info.up_token_id.clone(), m.info.down_token_id.clone()])
            .collect();

        if let Some(handle) = self._price_feed.take() {
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
                        .set_initial_position(market_id.clone(), up_bal, down_bal);
                    info!(
                        market = %market_symbol,
                        up_shares = %up_bal,
                        down_shares = %down_bal,
                        "Position initialized"
                    );
                }
                Err(e) => {
                    warn!(market = %market_symbol, error = %e, "Failed to fetch initial position");
                }
            }
        }
        match spawn_price_feed(WS_SUB_URL, token_ids, self.price_cache.clone()) {
            Ok(handle) => {
                self._price_feed = Some(handle);
                info!("Price feed started!!");
            }
            Err(e) => {
                error!(error = %e, "Failed to start price feed :(");
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

        // Seconds into each 15-minute window where we should refresh
        // 15 min = 900 seconds, refresh at 840-870 seconds (14:00-14:30)
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

            let (up_ask, down_ask, effective_target, overrides) = match price_snapshot {
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
                        );
                        extra.size_multiplier = overrides.size_multiplier;
                        extra.spacing_multiplier = overrides.spacing_multiplier;
                        extra.extra_offset = overrides.extra_offset;
                        (ranges, span_ms, sample_count, effective_target, overrides)
                    };

                    if self.should_log_trend(&market_id, status) {
                        match status {
                            TrendStatus::NotReady => {
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
                                        None,
                                        extra,
                                    )
                                );
                            }
                            TrendStatus::OutOfRange => {
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
                                        Some(self.config.trend_max_range),
                                        extra,
                                    )
                                );
                            }
                            TrendStatus::Ready => {
                                let rn_up_bid = summary.map(|s| s.rn_bid).unwrap_or_default();
                                let rn_up_ask = summary.map(|s| s.rn_ask).unwrap_or_default();
                                let naive_up_bid = summary.map(|s| s.naive_bid).unwrap_or_default();
                                let naive_up_ask = summary.map(|s| s.naive_ask).unwrap_or_default();
                                debug!(
                                    market = %market_symbol,
                                    "{}",
                                    format_trend_ready_log(
                                        up_bid,
                                        up_ask,
                                        down_bid,
                                        down_ask,
                                        ranges,
                                        span_ms,
                                        sample_count,
                                        summary,
                                        rn_up_bid,
                                        rn_up_ask,
                                        naive_up_bid,
                                        naive_up_ask,
                                        extra,
                                    )
                                );
                            }
                        }
                    }

                    if status != TrendStatus::Ready {
                        continue;
                    }

                    (up_ask, down_ask, effective_target, overrides)
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

            // Get position and open orders
            let (pos, open_orders) = {
                let ledger = self.ledger.read();
                (
                    ledger.effective_position(&market_id),
                    ledger.open_orders_for_market(&market_id),
                )
            };

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

            // Cancel stale orders first
            if !plan.cancellations.is_empty() {
                match self.cancel_orders(&plan.cancellations).await {
                    Ok(cancelled) => {
                        info!(market = %market_symbol, count = cancelled.len(), "Cancelled stale orders");
                        self.ledger.write().mark_orders_cancelled(&cancelled);
                    }
                    Err(e) => {
                        error!(market = %market_symbol, error = %e, "Failed to cancel orders");
                    }
                }
            }

            if !plan.orders.is_empty() {
                info!(
                    market = %market_symbol,
                    count = plan.orders.len(),
                    size_mult = %format_decimal(overrides.size_multiplier),
                    spacing_mult = %format_decimal(overrides.spacing_multiplier),
                    extra_offset = %format_decimal(overrides.extra_offset),
                    effective_target = %format_decimal(effective_target),
                    "Decision: place orders (trend ready)"
                );
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
                                    let token_id = match order.side {
                                        MarketSide::Up => &up_token_id,
                                        MarketSide::Down => &down_token_id,
                                    };
                                    self.ledger.write().record_order_placed(
                                        resp.order_id.clone(),
                                        market_id.clone(),
                                        token_id.clone(),
                                        order.side,
                                        order.size,
                                        order.price,
                                    );
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
                self.last_dry_run_log
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

            info!(
                "Placing order for {} shares @ {} for side {:?}",
                poly_size, poly_price, order.side
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

    async fn cancel_orders(&self, order_ids: &[String]) -> anyhow::Result<Vec<String>> {
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
) -> String {
    let max_range_str = max_range
        .map(|v| format_decimal(v))
        .unwrap_or_else(|| "-".to_string());
    let effective_max_range_str = extra
        .effective_max_range
        .map(format_decimal)
        .unwrap_or_else(|| "-".to_string());
    let start_mid = extra.start_mid.unwrap_or_default();
    let end_mid = extra.end_mid.unwrap_or_default();
    format!(
        "trend={} prices[up={}/{} down={}/{}] ranges[up={}/{} down={}/{} max={} eff_max={}] spreads[up={} down={}] age_ms={} swing_zone={} mid[start={:.5} end={:.5}] window[ms={} n={}] size_mult={} spacing_mult={} extra_offset={}",
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
        format_decimal(extra.size_multiplier),
        format_decimal(extra.spacing_multiplier),
        format_decimal(extra.extra_offset)
    )
}

fn format_trend_ready_log(
    up_bid: Decimal,
    up_ask: Decimal,
    down_bid: Decimal,
    down_ask: Decimal,
    ranges: crate::trend_window::TrendRanges,
    span_ms: i64,
    sample_count: usize,
    summary: Option<crate::trend_window::RnJdSummary>,
    rn_up_bid: f64,
    rn_up_ask: f64,
    naive_up_bid: f64,
    naive_up_ask: f64,
    extra: TrendLogExtra,
) -> String {
    let (rn_sigma, rn_lambda, rn_drift) = summary
        .map(|s| (s.sigma_b, s.jump_intensity, s.drift))
        .unwrap_or_default();
    let effective_max_range_str = extra
        .effective_max_range
        .map(format_decimal)
        .unwrap_or_else(|| "-".to_string());
    let start_mid = extra.start_mid.unwrap_or_default();
    let end_mid = extra.end_mid.unwrap_or_default();
    format!(
        "trend=ready prices[up={}/{} down={}/{}] ranges[up={}/{} down={}/{} eff_max={}] spreads[up={} down={}] age_ms={} swing_zone={} mid[start={:.5} end={:.5}] window[ms={} n={}] rn[sigma={:.5} lambda={:.5} drift={:.5} up={:.5}/{:.5} down={:.5}/{:.5}] naive[up={:.5}/{:.5} down={:.5}/{:.5}] size_mult={} spacing_mult={} extra_offset={} widen={}",
        format_decimal(up_bid),
        format_decimal(up_ask),
        format_decimal(down_bid),
        format_decimal(down_ask),
        format_decimal(ranges.up_bid),
        format_decimal(ranges.up_ask),
        format_decimal(ranges.down_bid),
        format_decimal(ranges.down_ask),
        effective_max_range_str,
        format_decimal(extra.up_spread),
        format_decimal(extra.down_spread),
        extra.max_age_ms.unwrap_or(-1),
        extra.swing_zone,
        start_mid,
        end_mid,
        span_ms,
        sample_count,
        rn_sigma,
        rn_lambda,
        rn_drift,
        rn_up_bid,
        rn_up_ask,
        1.0 - rn_up_ask,
        1.0 - rn_up_bid,
        naive_up_bid,
        naive_up_ask,
        1.0 - naive_up_ask,
        1.0 - naive_up_bid,
        format_decimal(extra.size_multiplier),
        format_decimal(extra.spacing_multiplier),
        format_decimal(extra.extra_offset),
        extra.high_vol_widen
    )
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
        }
    }
}

fn build_ladder_overrides(
    summary: Option<crate::trend_window::RnJdSummary>,
    up_bid: Decimal,
    up_ask: Decimal,
    _down_bid: Decimal,
    _down_ask: Decimal,
    mid_start_end: Option<(f64, f64)>,
    edge_threshold: Decimal,
    size_scale_min: Decimal,
    size_scale_max: Decimal,
    widen_factor: Decimal,
    tick_size: Decimal,
    pinned_low: Decimal,
    pinned_high: Decimal,
    drift_flicker_threshold: f64,
    high_vol_widen: bool,
) -> LadderOverrides {
    let mut overrides = LadderOverrides::default();
    overrides.size_multiplier = Decimal::ONE;
    overrides.spacing_multiplier = Decimal::ONE;
    overrides.extra_offset = Decimal::ZERO;

    if high_vol_widen {
        overrides.spacing_multiplier = widen_factor;
        if widen_factor > Decimal::ONE {
            overrides.extra_offset = tick_size * (widen_factor - Decimal::ONE);
        }
    }

    let Some(summary) = summary else {
        return overrides;
    };

    let rn_mid = (summary.rn_bid + summary.rn_ask) / 2.0;
    let up_bid_f = up_bid.to_f64().unwrap_or(0.0);
    let up_ask_f = up_ask.to_f64().unwrap_or(1.0);
    let edge_threshold_f = edge_threshold.to_f64().unwrap_or(0.0025);
    let tick_size_f = tick_size.to_f64().unwrap_or(0.01);

    let dist_to_bid = (rn_mid - up_bid_f).max(0.0);
    let dist_to_ask = (up_ask_f - rn_mid).max(0.0);

    if dist_to_ask <= edge_threshold_f {
        overrides.size_multiplier = size_scale_max;
    } else if dist_to_bid <= edge_threshold_f {
        overrides.size_multiplier = size_scale_min;
    }

    if let Some((start, end)) = mid_start_end {
        let move_abs = (end - start).abs();
        if move_abs < tick_size_f && summary.drift.abs() < drift_flicker_threshold {
            overrides.size_multiplier = size_scale_min;
        }
    }

    let mid_up = ((up_bid + up_ask) / dec!(2))
        .to_f64()
        .unwrap_or(0.5);
    let pinned_low_f = pinned_low.to_f64().unwrap_or(0.05);
    let pinned_high_f = pinned_high.to_f64().unwrap_or(0.95);
    if (mid_up <= pinned_low_f || mid_up >= pinned_high_f)
        && summary.drift.abs() >= drift_flicker_threshold
    {
        overrides.size_multiplier = size_scale_max;
    }

    overrides
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
