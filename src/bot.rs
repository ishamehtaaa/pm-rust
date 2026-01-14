use crate::config::ASSETS_BY_NAME;
use crate::config::Config;
use crate::constants::{SIZE_DP, round_size};
use crate::ladder::{LadderOrder, LadderPlan, OrderSide};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};
use crate::poller::{InventoryLedger, MarketSide, spawn_order_feed};
use crate::price_feed::{PriceCache, spawn_price_feed};
use crate::rn_jd;
use crate::trend_window::TrendWindow;
use chrono::Timelike;
use polymarket_client_sdk::auth::Credentials;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};

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
const MIN_ORDER_NOTIONAL: Decimal = dec!(1);
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
    last_order_status_check: HashMap<String, Instant>,
    last_model_cancel: HashMap<String, Instant>,
    last_balance_refresh: HashMap<String, Instant>,
    last_position_log: HashMap<String, PositionSnapshot>,
    last_pnl_log: HashMap<String, Instant>,

    ledger: Arc<RwLock<InventoryLedger>>,
    price_cache: Arc<RwLock<PriceCache>>,
    _order_feed: Option<tokio::task::JoinHandle<()>>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    active_market_ids: Arc<RwLock<Vec<String>>>,
    ws_client: Arc<AuthenticatedWsClient>,

    trend_windows: HashMap<String, TrendWindow>,
    trend_log_state: HashMap<String, TrendLogState>,
    last_missing_price_log: HashMap<String, Instant>,
    last_dry_run_log: HashMap<String, Instant>,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let market_cache = MarketCache::new(config.target_assets.clone(), config.target_duration);
        let price_cache = Arc::new(RwLock::new(PriceCache::default()));

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
            last_order_status_check: HashMap::new(),
            last_model_cancel: HashMap::new(),
            last_balance_refresh: HashMap::new(),
            last_position_log: HashMap::new(),
            last_pnl_log: HashMap::new(),
            active_market_ids,
            ledger,
            _order_feed: None,
            ws_client,
            price_cache,
            _price_feed: None,
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
            {
                let mut ledger = self.ledger.write();
                ledger.register_market(market_id_for_ledger.clone(), market_symbol);
                ledger.register_market_tokens(
                    market_id_for_ledger,
                    info.up_token_id.clone(),
                    info.down_token_id.clone(),
                );
            }
        }
        let mut removed_markets: Vec<(String, String, String)> = Vec::new();
        for (market_id, up_token_id, down_token_id) in previous_markets.iter() {
            if !self.markets.contains_key(market_id) {
                removed_markets.push((
                    market_id.clone(),
                    up_token_id.clone(),
                    down_token_id.clone(),
                ));
                self.trend_windows.remove(market_id);
                self.trend_log_state.remove(market_id);
                self.last_missing_price_log.remove(market_id);
                self.last_dry_run_log.remove(market_id);
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

        // Update active market IDs after processing all markets
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

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    async fn scan(&mut self) {
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

            let (up_bid, up_ask, down_bid, down_ask, up_age, down_age) = match price_snapshot {
                Some(snapshot) => snapshot,
                None => {
                    let should_log = self.should_log_missing_price(&market_id);
                    if should_log {
                        info!(market = %market_symbol, "Waiting for price data");
                    }
                    continue;
                }
            };

            let now_ms = Utc::now().timestamp_millis();
            let window = self.trend_windows.entry(market_id.clone()).or_default();
            window.record(
                now_ms,
                up_bid,
                up_ask,
                down_bid,
                down_ask,
                self.config.trend_window_secs,
            );

            if !window.is_ready(self.config.trend_window_secs) {
                if self.should_log_trend(&market_id, TrendStatus::NotReady) {
                    debug!(market = %market_symbol, "trend=warming");
                }
                continue;
            }

            let summary = match window.rn_jd_summary() {
                Some(summary) => summary,
                None => continue,
            };

            let max_age_ms = up_age.max(down_age) as i64;
            let ranges = window.ranges();
            let effective_range = ranges
                .up_bid
                .max(ranges.up_ask)
                .max(ranges.down_bid)
                .max(ranges.down_ask);
            let order_imbalance = window.order_imbalance(self.config.order_imbalance_window_secs);
            let paused_reason = if max_age_ms > self.config.max_price_age_ms {
                Some("stale")
            } else if summary.jump_intensity >= self.config.lambda_pause_threshold {
                Some("lambda_spike")
            } else if order_imbalance >= self.config.order_imbalance_threshold {
                Some("imbalance")
            } else {
                None
            };

            if let Some(reason) = paused_reason {
                let open_orders = self.ledger.read().open_orders_for_market(&market_id);
                if !open_orders.is_empty() && self.should_model_cancel(&market_id) {
                    let cancel_ids: Vec<String> =
                        open_orders.into_iter().map(|o| o.order_id).collect();
                    info!(
                        market = %market_symbol,
                        reason,
                        count = cancel_ids.len(),
                        "Model pause: cancelling open orders"
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
                continue;
            }

            let mid_up = (up_bid + up_ask) / dec!(2);
            let mid_down = (down_bid + down_ask) / dec!(2);
            let swing_zone =
                mid_up >= self.config.swing_zone_low && mid_up <= self.config.swing_zone_high;
            let effective_target = if swing_zone {
                self.config.shares_target_per_side * self.config.swing_zone_target_factor
            } else {
                self.config.shares_target_per_side
            };
            let spread_multiplier = if swing_zone {
                self.config.swing_zone_spread_multiplier
            } else {
                Decimal::ONE
            };

            let (pos, open_orders, allow_sell_up, allow_sell_down, pair_cost_cap) = {
                let ledger = self.ledger.read();
                (
                    ledger.effective_position(&market_id),
                    ledger.open_orders_for_market(&market_id),
                    ledger.has_buy_fill(&market_id, MarketSide::Up),
                    ledger.has_buy_fill(&market_id, MarketSide::Down),
                    self.config.target_total_cost,
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
            let net_inventory = pos.up_shares - pos.down_shares;

            let fair_x = summary.filtered_x + summary.drift;
            let risk_factor = self.config.rnjd_risk_aversion
                * summary.sigma_b.powi(2)
                * self.config.rnjd_time_horizon;
            let reservation_x = fair_x - net_inventory.to_f64().unwrap_or(0.0) * risk_factor;
            let liquidity_component = (2.0 / self.config.rnjd_k_liquidity)
                * (1.0 + self.config.rnjd_risk_aversion / self.config.rnjd_k_liquidity).ln();
            let spread_x =
                (risk_factor + liquidity_component) * spread_multiplier.to_f64().unwrap_or(1.0);
            let half_spread_x = spread_x / 2.0;
            let model_bid = rn_jd::sigmoid(reservation_x - half_spread_x);
            let model_ask = rn_jd::sigmoid(reservation_x + half_spread_x);

            let up_price = snap_bid_price(model_bid, up_ask, self.config.tick_size);
            let down_price = snap_bid_price(1.0 - model_ask, down_ask, self.config.tick_size);

            if self.should_log_trend(&market_id, TrendStatus::Ready) {
                debug!(
                    market = %market_symbol,
                    "trend=ready fair_p={:.5} bid/ask={:.5}/{:.5} sigma={:.5} lambda={:.5} drift={:.5} imbalance={:.2} swing_zone={} bid_px[up={} down={}]",
                    rn_jd::sigmoid(fair_x),
                    model_bid,
                    model_ask,
                    summary.sigma_b,
                    summary.jump_intensity,
                    summary.drift,
                    order_imbalance,
                    swing_zone,
                    format_decimal(up_price),
                    format_decimal(down_price)
                );
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
                debug!(
                    market = %market_symbol,
                    "position up={:.4} down={:.4} pending(up={:.4} down={:.4}) total(up={:.4} down={:.4}) target={:.2}",
                    pos.up_shares,
                    pos.down_shares,
                    pending_up,
                    pending_down,
                    total_up,
                    total_down,
                    effective_target
                );
            }

            if self.should_log_pnl(&market_id) {
                let mut ledger = self.ledger.write();
                let pnl = ledger.pnl_snapshot(&market_id, mid_up, mid_down);
                info!(
                    market = %market_symbol,
                    "pnl cash={:.4} inventory={:.4} total={:.4}",
                    pnl.cash,
                    pnl.inventory_value,
                    pnl.total
                );
            }

            let pending_sell_up: Decimal = open_orders
                .iter()
                .filter(|o| o.side == MarketSide::Up && o.order_side == OrderSide::Sell)
                .map(|o| o.remaining_size)
                .sum();
            let pending_sell_down: Decimal = open_orders
                .iter()
                .filter(|o| o.side == MarketSide::Down && o.order_side == OrderSide::Sell)
                .map(|o| o.remaining_size)
                .sum();
            let available_up = pos.up_shares - pending_sell_up;
            let available_down = pos.down_shares - pending_sell_down;
            let available_up = available_up.max(Decimal::ZERO);
            let available_down = available_down.max(Decimal::ZERO);

            let mut desired_orders = Vec::new();
            let up_room = effective_target - pos.up_shares;
            let down_room = effective_target - pos.down_shares;
            let up_edge = model_bid - up_bid.to_f64().unwrap_or(0.0);
            let down_edge = (1.0 - model_ask) - down_bid.to_f64().unwrap_or(0.0);
            let primary_side = if up_edge > down_edge {
                MarketSide::Up
            } else if down_edge > up_edge {
                MarketSide::Down
            } else if up_room > down_room {
                MarketSide::Up
            } else {
                MarketSide::Down
            };

            let (primary_price, secondary_price, primary_room, secondary_room) = match primary_side {
                MarketSide::Up => {
                    let cap = (pair_cost_cap - up_price).max(dec!(0.01));
                    let secondary = snap_bid_price(
                        cap.to_f64().unwrap_or(0.01),
                        down_ask,
                        self.config.tick_size,
                    );
                    (up_price, secondary, up_room, down_room)
                }
                MarketSide::Down => {
                    let cap = (pair_cost_cap - down_price).max(dec!(0.01));
                    let secondary = snap_bid_price(
                        cap.to_f64().unwrap_or(0.01),
                        up_ask,
                        self.config.tick_size,
                    );
                    (down_price, secondary, down_room, up_room)
                }
            };

            let add_order = |side: MarketSide,
                                 price: Decimal,
                                 room: Decimal,
                                 desired_orders: &mut Vec<LadderOrder>| {
                if room <= Decimal::ZERO {
                    return;
                }
                let projected = {
                    let ledger = self.ledger.read();
                    ledger.projected_pair_avg_cost(&market_id, side, self.config.order_size, price)
                };
                let allow_buy = projected.map(|cost| cost <= pair_cost_cap).unwrap_or(true);
                if !allow_buy {
                    debug!(
                        market = %market_symbol,
                        projected_cost = %projected.unwrap_or(Decimal::ZERO),
                        cap = %pair_cost_cap,
                        "Skipping buy: pair cost cap exceeded"
                    );
                    return;
                }
                desired_orders.push(LadderOrder {
                    side,
                    order_side: OrderSide::Buy,
                    price,
                    size: self.config.order_size,
                });
            };

            add_order(
                primary_side,
                primary_price,
                primary_room,
                &mut desired_orders,
            );
            let secondary_side = if primary_side == MarketSide::Up {
                MarketSide::Down
            } else {
                MarketSide::Up
            };
            add_order(
                secondary_side,
                secondary_price,
                secondary_room,
                &mut desired_orders,
            );

            let _ = allow_sell_up;
            let _ = allow_sell_down;
            let _ = available_up;
            let _ = available_down;

            let price_tolerance = cancellation_tolerance(effective_range);
            let plan = build_quote_plan(&open_orders, &desired_orders, price_tolerance);

            if plan.cancellations.is_empty() && plan.orders.is_empty() {
                continue;
            }

            // Cancel stale orders first
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
                                        order.order_side,
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
                        }
                        Err(e) => {
                            error!(market = %market_symbol, error = %e, "Batch order submission failed");
                        }
                    }
                }
            }
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

    fn should_log_pnl(&mut self, market_id: &str) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(10);
        match self.last_pnl_log.get(market_id) {
            Some(last) if now.duration_since(*last) < min_interval => false,
            _ => {
                self.last_pnl_log.insert(market_id.to_string(), now);
                true
            }
        }
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

            let mut adjusted_size = round_size(order.size);
            if order.order_side == OrderSide::Buy && order.price > Decimal::ZERO {
                let min_size = (MIN_ORDER_NOTIONAL / order.price)
                    .round_dp_with_strategy(SIZE_DP, RoundingStrategy::AwayFromZero);
                if adjusted_size < min_size {
                    info!(
                        "Bumping order size to satisfy min notional: size {} -> {} @ {}",
                        adjusted_size, min_size, order.price
                    );
                    adjusted_size = min_size;
                }
            }

            let poly_size = match PolyDecimal::try_from(adjusted_size.to_string().as_str()) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "Invalid size format");
                    continue;
                }
            };

            info!(
                "Placing order {} {} shares @ {} for side {:?}",
                match order.order_side {
                    OrderSide::Buy => "BUY",
                    OrderSide::Sell => "SELL",
                },
                poly_size,
                poly_price,
                order.side
            );

            let clob_side = match order.order_side {
                OrderSide::Buy => ClobSide::Buy,
                OrderSide::Sell => ClobSide::Sell,
            };
            let signable = match self
                .client
                .limit_order()
                .token_id(token_id)
                .price(poly_price)
                .size(poly_size)
                .side(clob_side)
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

fn format_decimal(value: Decimal) -> String {
    value.round_dp(5).to_string()
}

fn cancellation_tolerance(effective_range: Decimal) -> Decimal {
    if effective_range <= dec!(0.02) {
        return dec!(0.04);
    }
    dec!(0.15)
}

fn snap_bid_price(price: f64, ask: Decimal, tick: Decimal) -> Decimal {
    let tick = tick.max(dec!(0.01));
    let ask_f = ask.to_f64().unwrap_or(1.0);
    let cap = (ask_f - tick.to_f64().unwrap_or(0.01)).max(0.01);
    let capped = price.min(cap).max(0.01);
    let capped_dec = Decimal::from_f64(capped).unwrap_or(dec!(0.01));
    let ticks = (capped_dec / tick).floor();
    (ticks * tick).max(dec!(0.01))
}

fn build_quote_plan(
    open_orders: &[crate::ladder::OpenOrderInfo],
    desired: &[LadderOrder],
    price_tolerance: Decimal,
) -> LadderPlan {
    let mut plan = LadderPlan::default();
    let mut desired_by_side: HashMap<(MarketSide, OrderSide), &LadderOrder> = HashMap::new();
    for order in desired {
        desired_by_side.insert((order.side, order.order_side), order);
    }

    let mut matched = HashMap::new();

    for open in open_orders {
        if let Some(desired_order) = desired_by_side.get(&(open.side, open.order_side)) {
            let price_close = (open.price - desired_order.price).abs() <= price_tolerance;
            if price_close && open.remaining_size == desired_order.size {
                matched.insert((open.side, open.order_side), true);
                continue;
            }
        }
        plan.cancellations.push(open.order_id.clone());
    }

    for order in desired {
        if !matched.contains_key(&(order.side, order.order_side)) {
            plan.orders.push(order.clone());
        }
    }

    plan
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TrendStatus {
    NotReady,
    Ready,
}

#[derive(Debug, Clone, Copy)]
struct TrendLogState {
    status: TrendStatus,
    last_log: Instant,
}
