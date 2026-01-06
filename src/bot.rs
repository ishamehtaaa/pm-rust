use crate::config::{BotSettings, Config};
use crate::market_cache::MarketCache;
use crate::models::{
    ExecutionState, MarketIds, MarketInfo, MarketInventory, MarketLookup, MarketState, OrderEvent,
    RestingOrder, SecondLegParams, Side,
};

use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::{DateTime, Utc};
use futures_util::future::join_all;
use futures_util::stream::StreamExt;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{OrderType, Side as ClobSide, SignatureType};
use polymarket_client_sdk::clob::ws::{self, BookUpdate, WsMessage};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::data::{
    Client as DataClient,
    types::{request::PositionsRequest, response::Position},
};
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

fn round_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
}

fn floor_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero)
}

fn min_shares_for_notional(price: Decimal, min_notional: Decimal) -> Decimal {
    if price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (min_notional / price).round_dp_with_strategy(0, rust_decimal::RoundingStrategy::AwayFromZero)
}

#[derive(Debug)]
struct PriceUpdate {
    token_id: String,
    bid: Decimal,
    ask: Decimal,
}

#[derive(Debug)]
struct TakerResult {
    gamma_id: String,
    side: Side,
    size: Decimal,
    price: Decimal,
    success: bool,
    balance_error: bool,
}

// Result of a successful Maker placement
#[derive(Debug)]
struct MakerResult {
    gamma_id: String,
    token_id: String,
    order_id: String,
    price: Decimal,
    size: Decimal,
    side: Side,
    second_leg: Option<SecondLegParams>,
}

pub struct LeggingBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,

    markets: HashMap<String, MarketState>,
    // Optimized O(1) lookup for price updates
    token_to_gamma: HashMap<String, String>,
    condition_to_gamma: HashMap<String, String>,
    state: HashMap<String, ExecutionState>,

    order_rx: mpsc::Receiver<OrderEvent>,
    order_tx: mpsc::Sender<OrderEvent>,
    price_rx: mpsc::Receiver<PriceUpdate>,
    price_tx: mpsc::Sender<PriceUpdate>,
    taker_rx: mpsc::Receiver<TakerResult>,
    taker_tx: mpsc::Sender<TakerResult>,
    // New channel for non-blocking maker confirmation
    maker_rx: mpsc::Receiver<MakerResult>,
    maker_tx: mpsc::Sender<MakerResult>,
    positions_rx: mpsc::Receiver<Vec<Position>>,

    ws_market: ws::Client,
    ws_user: ws::Client<Authenticated<Normal>>,
    ws_tasks: Vec<tokio::task::JoinHandle<()>>,

    current_window_end: Option<DateTime<Utc>>,
}

impl LeggingBot {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing Optimized Legging Bot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)?;
        let signer = signer.with_chain_id(Some(POLYGON));

        let (sig_type, funder, trader) = if config.polymarket_proxy_address.trim().is_empty() {
            let addr = Address::from_str(&signer.address().to_string())?;
            (SignatureType::Eoa, None, addr)
        } else {
            let addr = Address::from_str(config.polymarket_proxy_address.trim())?;
            (SignatureType::Proxy, Some(addr), addr)
        };

        let key_client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?;
        let creds = key_client.create_or_derive_api_key(&signer, None).await?;
        drop(key_client);

        let mut auth = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer)
            .credentials(creds.clone());

        if let Some(f) = funder {
            auth = auth.funder(f).signature_type(sig_type);
        }

        let client = Arc::new(auth.authenticate().await?);
        let ws_market = ws::Client::default();
        let ws_user = ws::Client::default().authenticate(creds, trader.clone())?;

        let data_client = DataClient::default();
        let (positions_tx, positions_rx) = mpsc::channel(64);
        let positions_interval = config.bot_settings.positions_poll_interval;
        Self::spawn_positions_poller(
            data_client,
            positions_tx,
            trader.clone(),
            positions_interval,
        );
        info!("Automated positions polling every {:?}", positions_interval);

        let (order_tx, order_rx) = mpsc::channel(256);
        let (price_tx, price_rx) = mpsc::channel(2048); // Increased buffer
        let (taker_tx, taker_rx) = mpsc::channel(64);
        let (maker_tx, maker_rx) = mpsc::channel(128);

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            token_to_gamma: HashMap::new(),
            condition_to_gamma: HashMap::new(),
            state: HashMap::new(),
            order_rx,
            order_tx,
            price_rx,
            price_tx,
            taker_rx,
            taker_tx,
            maker_rx,
            maker_tx,
            positions_rx,
            ws_market,
            ws_user,
            ws_tasks: Vec::new(),
            current_window_end: None,
        })
    }

    fn settings(&self) -> &BotSettings {
        &self.config.bot_settings
    }

    fn clamp_price(&self, price: Decimal) -> Decimal {
        let settings = self.settings();
        price
            .round_dp(2)
            .max(settings.min_price)
            .min(settings.max_price)
    }

    fn min_shares_for_notional(&self, price: Decimal) -> Decimal {
        min_shares_for_notional(price, self.settings().min_notional)
    }

    fn asset_name(&self, gamma_id: &str) -> String {
        self.markets
            .get(gamma_id)
            .map(|m| m.info.asset.to_uppercase())
            .unwrap_or_else(|| gamma_id.to_string())
    }

    fn inventory_snapshot(&self, gamma_id: &str) -> MarketInventory {
        self.state
            .get(gamma_id)
            .map(|st| st.inventory().clone())
            .unwrap_or_default()
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;

        let mut status_tick = tokio::time::interval(self.settings().status_interval);
        let mut ladder_tick = tokio::time::interval(self.settings().ladder_interval);
        let mut rollover_tick = tokio::time::interval(self.settings().rollover_interval);
        let mut cleanup_tick = tokio::time::interval(self.settings().cleanup_interval);

        loop {
            tokio::select! {
                Some(ev) = self.order_rx.recv() => self.on_order_event(ev).await,
                Some(res) = self.taker_rx.recv() => self.on_taker_result(res),
                Some(mk) = self.maker_rx.recv() => self.on_maker_result(mk),
                Some(upd) = self.price_rx.recv() => self.on_price_update(upd).await,
                maybe_snapshot = self.positions_rx.recv() => {
                    if let Some(snapshot) = maybe_snapshot {
                        self.sync_inventory_from_positions(snapshot);
                    } else {
                        warn!("Positions poll task terminated, breaking run loop");
                        break;
                    }
                }
                _ = ladder_tick.tick() => self.maintain_all_ladders().await,
                _ = rollover_tick.tick() => self.check_rollover().await,
                _ = cleanup_tick.tick() => self.cleanup_history(),
                _ = status_tick.tick() => self.log_status(),
            }
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Maintenance & Cleanup
    // ───────────────────────────────────────────────────────────────────────────

    fn cleanup_history(&mut self) {
        let now = Instant::now();
        let settings = self.settings();
        let retention = settings.order_history_retention;
        let prefill_retention = settings.prefill_retention;

        for st in self.state.values_mut() {
            st.processed_trade_ids
                .retain(|_, time| now.duration_since(*time) < retention);
            st.prefilled
                .retain(|_, (_, time)| now.duration_since(*time) < prefill_retention);
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Event handlers
    // ───────────────────────────────────────────────────────────────────────────

    fn on_taker_result(&mut self, res: TakerResult) {
        let asset = self.asset_name(&res.gamma_id);
        if let Some(st) = self.state.get_mut(&res.gamma_id) {
            st.taker_in_flight = false;
            st.balance_error = res.balance_error;

            if res.success && res.size > Decimal::ZERO {
                st.record_fill(res.side, res.size, res.price);
                let inv = st.inventory();
                info!(
                    "[{}] Taker fill confirmed: {} shares | inv: {}/{}",
                    asset,
                    res.size,
                    inv.up_shares.round_dp(2),
                    inv.down_shares.round_dp(2)
                );
            }
        }
    }

    fn on_maker_result(&mut self, res: MakerResult) {
        // FIX: Capture the name string here, before self.state is borrowed mutably
        let asset_name = self.asset_name(&res.gamma_id);

        if let Some(st) = self.state.get_mut(&res.gamma_id) {
            let prefilled = st
                .prefilled
                .remove(&res.order_id)
                .map(|(amt, _)| amt)
                .unwrap_or(Decimal::ZERO);

            if prefilled > Decimal::ZERO {
                st.record_fill(res.side, prefilled, res.price);
                let inv = st.inventory();
                info!(
                    "[{}] Fill (early): {} {:?} @ {} | order {} | inv: {}/{}",
                    asset_name,
                    prefilled,
                    res.side,
                    res.price,
                    res.order_id,
                    inv.up_shares.round_dp(2),
                    inv.down_shares.round_dp(2)
                );
            }

            st.quotes.side_orders_mut(res.side).push(RestingOrder {
                order_id: res.order_id,
                token_id: res.token_id,
                price: res.price,
                size: res.size,
                filled: prefilled,
                posted_at: Instant::now(),
                second_leg: res.second_leg,
            });
        }
    }

    async fn on_order_event(&mut self, ev: OrderEvent) {
        let gamma_id = ev.gamma_id.clone();
        let asset = self.asset_name(&gamma_id);

        let (fill_delta, second_leg_params) = {
            let st = match self.state.get_mut(&gamma_id) {
                Some(s) => s,
                None => return,
            };

            match ev.msg_type.as_str() {
                "CANCELLATION" => {
                    st.quotes.clear_order_by_id(&ev.order_id);
                    (Decimal::ZERO, None)
                }
                "TRADE_FILL" | "UPDATE" => {
                    let mut delta = Decimal::ZERO;
                    let mut params = None;

                    if let Some((o, _)) = st.quotes.find_order_mut_by_id(&ev.order_id) {
                        if ev.size_matched > o.filled {
                            delta = ev.size_matched - o.filled;
                            o.filled = ev.size_matched;
                            if o.is_fully_filled() {
                                params = o.second_leg.clone();
                            }
                        }
                    } else {
                        let entry = st
                            .prefilled
                            .entry(ev.order_id.clone())
                            .or_insert((Decimal::ZERO, Instant::now()));

                        if ev.size_matched > entry.0 {
                            delta = ev.size_matched - entry.0;
                            entry.0 = ev.size_matched;
                        }
                    }

                    if delta > Decimal::ZERO {
                        if let Some((o, _)) = st.quotes.find_order_by_id(&ev.order_id) {
                            if o.is_fully_filled() {
                                st.quotes.clear_order_by_id(&ev.order_id);
                            }
                        }
                    }

                    (delta, params)
                }
                _ => (Decimal::ZERO, None),
            }
        };

        // Step 2: Update persistent inventory and trigger second leg if necessary
        if fill_delta > Decimal::ZERO {
            // Scope ensures we drop the mutable borrow before calling second leg
            if let Some(st) = self.state.get_mut(&gamma_id) {
                st.record_fill(ev.side, fill_delta, ev.price);
                let inv = st.inventory();
                info!(
                    "[{}] Fill: {} {:?} @ {} | Net Inv: {}/{}",
                    asset,
                    fill_delta,
                    ev.side,
                    ev.price,
                    inv.up_shares.round_dp(2),
                    inv.down_shares.round_dp(2)
                );
            }

            if let Some(params) = second_leg_params {
                self.maybe_second_leg(&gamma_id, Some(params)).await;
            }
        }
    }

    async fn on_price_update(&mut self, upd: PriceUpdate) {
        // Optimized: O(1) Lookup
        let gamma_id = match self.token_to_gamma.get(&upd.token_id) {
            Some(id) => id.clone(),
            None => return,
        };

        let is_up = self
            .markets
            .get(&gamma_id)
            .map(|m| m.info.ids.up_token == upd.token_id)
            .unwrap_or(false);

        if let Some(st) = self.state.get_mut(&gamma_id) {
            if is_up {
                st.up_bid = Some(upd.bid);
                st.up_ask = Some(upd.ask);
            } else {
                st.down_bid = Some(upd.bid);
                st.down_ask = Some(upd.ask);
            }
        }

        self.maybe_rebalance(&gamma_id).await;
    }

    fn sync_inventory_from_positions(&mut self, positions: Vec<Position>) {
        if positions.is_empty() {
            debug!("Positions poll returned no entries");
            return;
        }

        let mut aggregated: HashMap<String, MarketInventory> = HashMap::new();
        let mut seen = HashSet::new();

        for position in positions {
            let condition = position.condition_id.as_str();
            let gamma_id = match self.condition_to_gamma.get(condition) {
                Some(id) => id.clone(),
                None => continue,
            };

            let side = match self
                .markets
                .get(&gamma_id)
                .and_then(|m| m.info.ids.side_for_token(&position.asset))
            {
                Some(s) => s,
                None => continue,
            };

            seen.insert(gamma_id.clone());
            let inventory = aggregated.entry(gamma_id.clone()).or_default();
            let cost = position.avg_price * position.size;

            match side {
                Side::Up => {
                    inventory.up_shares += position.size;
                    inventory.up_cost_basis += cost;
                }
                Side::Down => {
                    inventory.down_shares += position.size;
                    inventory.down_cost_basis += cost;
                }
            }
        }

        let mut summary = Vec::new();
        for (gamma_id, inventory) in aggregated {
            let asset = self.asset_name(&gamma_id);
            summary.push(format!(
                "[{}] positions | up: {} @ {:?} | down: {} @ {:?}",
                asset,
                inventory.up_shares.round_dp(2),
                inventory.avg_price(Side::Up).map(|p| p.round_dp(3)),
                inventory.down_shares.round_dp(2),
                inventory.avg_price(Side::Down).map(|p| p.round_dp(3)),
            ));
            let st = self.state.entry(gamma_id.clone()).or_default();
            *st.inventory_mut() = inventory;
        }

        for gamma_id in self.markets.keys() {
            if seen.contains(gamma_id) {
                continue;
            }
            if let Some(st) = self.state.get_mut(gamma_id) {
                let inv = st.inventory();
                if inv.up_shares != Decimal::ZERO || inv.down_shares != Decimal::ZERO {
                    *st.inventory_mut() = MarketInventory::default();
                    let asset = self.asset_name(gamma_id);
                    summary.push(format!("[{}] positions cleared (no open holdings)", asset));
                }
            }
        }

        for line in summary {
            info!("{}", line);
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Decision Logic
    // ───────────────────────────────────────────────────────────────────────────

    async fn maybe_rebalance(&mut self, gamma_id: &str) {
        let cfg = &self.config.legging_config;
        let settings = self.settings();
        let inv = self.inventory_snapshot(gamma_id);

        let params = {
            let st = match self.state.get(gamma_id) {
                Some(s) => s,
                None => return,
            };

            if !st.can_send_taker(settings.taker_cooldown, settings.balance_error_cooldown) {
                return;
            }

            let imb = inv.imbalance();
            if imb.abs() < dec!(1.0) {
                return;
            }

            let (needed, excess_side) = if imb > dec!(0) {
                (Side::Down, Side::Up)
            } else {
                (Side::Up, Side::Down)
            };

            let ask = match st.ask_for_side(needed) {
                Some(a) => a,
                None => return,
            };

            let excess_avg = inv.avg_price(excess_side).unwrap_or(dec!(0.50));
            let combined = excess_avg + ask;

            if combined > cfg.target_combined || dec!(1.0) - combined < dec!(0.005) {
                return;
            }

            let current_shares = match needed {
                Side::Up => inv.up_shares,
                Side::Down => inv.down_shares,
            };
            let resting: Decimal = st
                .quotes
                .side_orders(needed)
                .iter()
                .map(|o| o.remaining())
                .sum();
            let capacity = cfg.target_shares_per_market - (current_shares + resting);

            if capacity < settings.min_order_size {
                return;
            }

            let size = floor_size(imb.abs().min(capacity)).max(self.min_shares_for_notional(ask));
            if size < settings.min_order_size || size > capacity {
                return;
            }

            Some((needed, size, self.clamp_price(ask + settings.price_tick)))
        };

        if let Some((side, size, price)) = params {
            self.send_taker(gamma_id, side, size, price).await;
        }
    }

    async fn maybe_second_leg(&mut self, gamma_id: &str, precomputed: Option<SecondLegParams>) {
        let cfg = &self.config.legging_config;
        let settings = self.settings();
        let inv = self.inventory_snapshot(gamma_id);

        let params = {
            let st = match self.state.get(gamma_id) {
                Some(s) => s,
                None => return,
            };

            if !st.can_send_taker(settings.taker_cooldown, settings.balance_error_cooldown) {
                return;
            }

            let imb = inv.imbalance();
            if imb.abs() < dec!(1.0) {
                return;
            }

            let (needed, first_side) = if imb > dec!(0) {
                (Side::Down, Side::Up)
            } else {
                (Side::Up, Side::Down)
            };

            let first_avg = inv.avg_price(first_side).unwrap_or(dec!(0.50));
            let max_price = match &precomputed {
                Some(p) if p.side == needed => p.max_price,
                _ => cfg.target_combined - first_avg,
            };

            let ask = match st.ask_for_side(needed) {
                Some(a) if a <= max_price => a,
                _ => return,
            };

            let taker_price = self.clamp_price(ask + settings.price_tick);
            if first_avg + taker_price > cfg.target_combined {
                return;
            }
            // Safety check for impossible arb
            if dec!(1.0) - (first_avg + taker_price) < dec!(0.005) {
                return;
            }

            let current_shares = match needed {
                Side::Up => inv.up_shares,
                Side::Down => inv.down_shares,
            };

            let resting: Decimal = st
                .quotes
                .side_orders(needed)
                .iter()
                .map(|o| o.remaining())
                .sum();
            let capacity = cfg.target_shares_per_market - (current_shares + resting);

            if capacity < settings.min_order_size {
                return;
            }

            let size =
                floor_size(imb.abs().min(capacity)).max(self.min_shares_for_notional(taker_price));
            if size < settings.min_order_size || size > capacity {
                return;
            }

            Some((needed, size, taker_price))
        };

        if let Some((side, size, price)) = params {
            self.send_taker(gamma_id, side, size, price).await;
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Optimized Network Execution (Non-Blocking)
    // ───────────────────────────────────────────────────────────────────────────

    async fn send_taker(&mut self, gamma_id: &str, side: Side, size: Decimal, price: Decimal) {
        let token_id = match self.markets.get(gamma_id) {
            Some(m) => m.info.ids.token_for_side(side).to_string(),
            None => return,
        };
        let asset = self.asset_name(gamma_id);

        let cancel_ids: Vec<String> = {
            let st = match self.state.get_mut(gamma_id) {
                Some(s) => s,
                None => return,
            };

            if st.taker_in_flight {
                return;
            }

            st.taker_in_flight = true;
            st.last_taker_time = Some(Instant::now());

            st.quotes
                .side_orders_mut(side)
                .drain(..)
                .map(|o| o.order_id)
                .collect()
        };

        info!(
            "[{}] Taker: {} {:?} @ {} (cancelling {} orders)",
            asset,
            size,
            side,
            price,
            cancel_ids.len()
        );

        let client = self.client.clone();
        let signer = self.signer.clone();
        let taker_tx = self.taker_tx.clone();
        let gamma_id = gamma_id.to_string();
        let dry_run = self.config.dry_run;

        // Spawn detached task so the main loop doesn't block on cancels
        tokio::spawn(async move {
            // Optimized: Execute cancels in parallel
            if !cancel_ids.is_empty() {
                let cancels: Vec<_> = cancel_ids
                    .iter()
                    .map(|oid| client.cancel_order(oid))
                    .collect();
                join_all(cancels).await;
            }

            if dry_run {
                info!("[{}] [DRY] taker {} {:?} @ {}", asset, size, side, price);
                let _ = taker_tx
                    .send(TakerResult {
                        gamma_id,
                        side,
                        size,
                        price,
                        success: true,
                        balance_error: false,
                    })
                    .await;
                return;
            }

            // String conversions moved to async task
            let size_p = match PolyDecimal::try_from(size.to_string().as_str()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to convert size: {}", asset, e);
                    let _ = taker_tx
                        .send(TakerResult {
                            gamma_id,
                            side,
                            size,
                            price,
                            success: false,
                            balance_error: false,
                        })
                        .await;
                    return;
                }
            };
            let price_p = match PolyDecimal::try_from(price.to_string().as_str()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to convert price: {}", asset, e);
                    let _ = taker_tx
                        .send(TakerResult {
                            gamma_id,
                            side,
                            size,
                            price,
                            success: false,
                            balance_error: false,
                        })
                        .await;
                    return;
                }
            };

            let order = match client
                .limit_order()
                .token_id(&token_id)
                .price(price_p)
                .size(size_p)
                .side(ClobSide::Buy)
                .order_type(OrderType::FOK)
                .build()
                .await
            {
                Ok(v) => v,
                Err(_) => {
                    let _ = taker_tx
                        .send(TakerResult {
                            gamma_id,
                            side,
                            size,
                            price,
                            success: false,
                            balance_error: false,
                        })
                        .await;
                    return;
                }
            };

            let signed = match client.sign(&signer, order).await {
                Ok(v) => v,
                Err(_) => {
                    let _ = taker_tx
                        .send(TakerResult {
                            gamma_id,
                            side,
                            size,
                            price,
                            success: false,
                            balance_error: false,
                        })
                        .await;
                    return;
                }
            };

            match client.post_order(signed).await {
                Ok(r) => {
                    let matched = r.taking_amount;
                    info!(
                        "[{}] Taker filled: {}/{} {:?} @ {} ({})",
                        asset, matched, size, side, price, r.order_id
                    );
                    let _ = taker_tx
                        .send(TakerResult {
                            gamma_id,
                            side,
                            size: matched,
                            price,
                            success: true,
                            balance_error: false,
                        })
                        .await;
                }
                Err(e) => {
                    let err = e.to_string();
                    let bal_err = err.contains("balance") || err.contains("allowance");
                    warn!("[{}] Taker rejected: {}", asset, err);
                    let _ = taker_tx
                        .send(TakerResult {
                            gamma_id,
                            side,
                            size,
                            price,
                            success: false,
                            balance_error: bal_err,
                        })
                        .await;
                }
            }
        });
    }

    fn build_positions_request(user: &Address) -> Option<PositionsRequest> {
        let builder = PositionsRequest::builder().user(user.clone());
        let builder = match builder.limit(500) {
            Ok(b) => b,
            Err(e) => {
                warn!("Positions request limit invalid: {}", e);
                return None;
            }
        };

        Some(builder.build())
    }

    fn spawn_positions_poller(
        client: DataClient,
        tx: mpsc::Sender<Vec<Position>>,
        user: Address,
        interval_duration: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);

            loop {
                interval.tick().await;
                let request = match Self::build_positions_request(&user) {
                    Some(req) => req,
                    None => continue,
                };

                match client.positions(&request).await {
                    Ok(positions) => {
                        if positions.is_empty() {
                            continue;
                        }
                        if tx.send(positions).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => warn!("Positions poll failed: {}", e),
                }
            }
        })
    }

    async fn maintain_all_ladders(&mut self) {
        let ids: Vec<String> = self.markets.keys().cloned().collect();
        for id in ids {
            self.maintain_ladder(&id).await;
        }
    }

    async fn maintain_ladder(&mut self, gamma_id: &str) {
        let imbalance_threshold = self.settings().imbalance_threshold;
        let min_order_size = self.settings().min_order_size;
        let cfg = &self.config.legging_config;
        let target_shares = cfg.target_shares_per_market;
        let target_combined = cfg.target_combined;

        let market = match self.markets.get(gamma_id) {
            Some(m) => m.info.clone(),
            None => return,
        };

        let inv = self.inventory_snapshot(gamma_id);

        let (_up_ask, _down_ask, up_target, down_target, up_cap, down_cap) = {
            let st = match self.state.get(gamma_id) {
                Some(s) => s,
                None => return,
            };

            let (up_ask, down_ask) = match (st.up_ask, st.down_ask) {
                (Some(u), Some(d)) => (u, d),
                _ => return,
            };

            // Calculate targets based on held inventory avg or current market
            let up_target = inv
                .avg_price(Side::Down)
                .map(|avg| target_combined - avg)
                .unwrap_or(target_combined - down_ask);

            let down_target = inv
                .avg_price(Side::Up)
                .map(|avg| target_combined - avg)
                .unwrap_or(target_combined - up_ask);

            let up_resting: Decimal = st.quotes.up_orders.iter().map(|o| o.remaining()).sum();
            let down_resting: Decimal = st.quotes.down_orders.iter().map(|o| o.remaining()).sum();

            let up_cap = target_shares - (inv.up_shares + up_resting);
            let down_cap = target_shares - (inv.down_shares + down_resting);

            (up_ask, down_ask, up_target, down_target, up_cap, down_cap)
        };

        let imb = inv.imbalance();

        if imb <= imbalance_threshold && up_cap >= min_order_size {
            self.maintain_side_ladder(gamma_id, Side::Up, &market.ids, up_target, up_cap)
                .await;
        } else {
            self.cancel_side(gamma_id, Side::Up).await;
        }

        if imb >= -imbalance_threshold && down_cap >= min_order_size {
            self.maintain_side_ladder(gamma_id, Side::Down, &market.ids, down_target, down_cap)
                .await;
        } else {
            self.cancel_side(gamma_id, Side::Down).await;
        }
    }

    async fn maintain_side_ladder(
        &mut self,
        gamma_id: &str,
        side: Side,
        ids: &MarketIds,
        base_price: Decimal,
        capacity: Decimal,
    ) {
        let cfg = &self.config.legging_config;
        let min_price = self.settings().min_price;
        let max_price = self.settings().max_price;
        let price_tick = self.settings().price_tick;
        let levels_per_side = self.settings().levels_per_side;
        let min_order_size = self.settings().min_order_size;
        let min_notional = self.settings().min_notional;
        let base = self.clamp_price(base_price);

        if base < min_price + price_tick || base > max_price - price_tick {
            self.cancel_side(gamma_id, side).await;
            return;
        }

        let mut levels: Vec<Decimal> = Vec::new();
        let mut cap_left = capacity;
        for i in 0..levels_per_side {
            let p = self.clamp_price(base - price_tick * Decimal::from(i as u32));
            if p < min_price + price_tick || cap_left < min_order_size {
                break;
            }
            levels.push(p);
            cap_left -= cfg.shares_per_trade.min(cap_left);
        }

        if levels.is_empty() {
            self.cancel_side(gamma_id, side).await;
            return;
        }

        let top = levels[0];
        let bottom = levels[levels.len() - 1];
        let thresh = cfg.requote_threshold;

        // -------- Cancel stale orders (UNCHANGED) --------
        let (keep, stale): (Vec<RestingOrder>, Vec<String>) = {
            let st = match self.state.get_mut(gamma_id) {
                Some(s) => s,
                None => return,
            };

            let existing = std::mem::take(st.quotes.side_orders_mut(side));
            let mut k = Vec::new();
            let mut s = Vec::new();

            for o in existing {
                if o.price > top + thresh || o.price < bottom - thresh {
                    s.push(o.order_id.clone());
                } else {
                    k.push(o);
                }
            }

            (k, s)
        };

        if !stale.is_empty() {
            let client = self.client.clone();
            tokio::spawn(async move {
                let futures: Vec<_> = stale.iter().map(|oid| client.cancel_order(oid)).collect();
                join_all(futures).await;
            });
        }

        if let Some(st) = self.state.get_mut(gamma_id) {
            *st.quotes.side_orders_mut(side) = keep.clone();
        }

        // -------- Calculate uncovered levels (UNCHANGED) --------
        let covered: std::collections::HashSet<Decimal> =
            keep.iter().map(|o| o.price.round_dp(2)).collect();

        let uncovered: Vec<Decimal> = levels
            .into_iter()
            .filter(|p| !covered.iter().any(|c| (*c - *p).abs() <= thresh))
            .collect();

        if uncovered.is_empty() {
            return;
        }

        // -------- Order posting logic (UNCHANGED) --------
        let second_side = match side {
            Side::Up => Side::Down,
            Side::Down => Side::Up,
        };
        let token_id = ids.token_for_side(side).to_string();
        let second_token_id = ids.token_for_side(second_side).to_string();

        let client = self.client.clone();
        let signer = self.signer.clone();
        let maker_tx = self.maker_tx.clone();
        let gamma_id = gamma_id.to_string();
        let dry_run = self.config.dry_run;
        let shares_per_trade = cfg.shares_per_trade;
        let target_combined = cfg.target_combined;

        tokio::spawn(async move {
            let mut handles = Vec::new();

            for price in uncovered {
                let size = shares_per_trade.max(min_shares_for_notional(price, min_notional));

                let t_id = token_id.clone();
                let g_id = gamma_id.clone();
                let c = client.clone();
                let s = signer.clone();
                let m_tx = maker_tx.clone();

                let second_leg = Some(SecondLegParams {
                    token_id: second_token_id.clone(),
                    side: second_side,
                    max_price: target_combined - price,
                });

                if dry_run {
                    let now = Utc::now();
                    let ts = now
                        .timestamp_nanos_opt()
                        .unwrap_or(now.timestamp_millis() * 1_000_000);
                    let _ = m_tx
                        .send(MakerResult {
                            gamma_id: g_id,
                            token_id: t_id,
                            order_id: format!("dry-{}", ts),
                            price,
                            size,
                            side,
                            second_leg,
                        })
                        .await;
                    continue;
                }

                handles.push(async move {
                    let size_p =
                        PolyDecimal::try_from(round_size(size).to_string().as_str()).ok()?;
                    let price_p = PolyDecimal::try_from(price.to_string().as_str()).ok()?;

                    let order = c
                        .limit_order()
                        .token_id(&t_id)
                        .price(price_p)
                        .size(size_p)
                        .side(ClobSide::Buy)
                        .order_type(OrderType::GTC)
                        .build()
                        .await
                        .ok()?;

                    let signed = c.sign(&s, order).await.ok()?;

                    if let Ok(r) = c.post_order(signed).await {
                        let _ = m_tx
                            .send(MakerResult {
                                gamma_id: g_id,
                                token_id: t_id,
                                order_id: r.order_id,
                                price,
                                size,
                                side,
                                second_leg,
                            })
                            .await;
                    }
                    Some(())
                });
            }

            join_all(handles).await;
        });
    }

    async fn cancel_side(&mut self, gamma_id: &str, side: Side) {
        let ids: Vec<String> = {
            let st = match self.state.get_mut(gamma_id) {
                Some(s) => s,
                None => return,
            };
            st.quotes
                .side_orders_mut(side)
                .drain(..)
                .map(|o| o.order_id)
                .collect()
        };

        if ids.is_empty() {
            return;
        }

        let client = self.client.clone();
        tokio::spawn(async move {
            let futures: Vec<_> = ids.iter().map(|oid| client.cancel_order(oid)).collect();
            join_all(futures).await;
        });
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Market discovery & WS
    // ───────────────────────────────────────────────────────────────────────────

    pub async fn discover_markets(&mut self) {
        for t in self.ws_tasks.drain(..) {
            t.abort();
        }
        let now = Utc::now();
        let active_markets = match self.market_cache.get_markets(now).await {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to fetch markets: {}", e);
                return;
            }
        };

        self.markets.clear();
        self.state.clear();
        self.token_to_gamma.clear();
        self.condition_to_gamma.clear();

        if active_markets.is_empty() {
            warn!("No active markets found");
            self.current_window_end = None;
            return;
        }

        let window_end = match active_markets.iter().map(|m| m.end_time).min() {
            Some(end) => end,
            None => {
                warn!("Could not determine window end");
                self.current_window_end = None;
                return;
            }
        };

        let window_markets: Vec<MarketInfo> = active_markets
            .into_iter()
            .filter(|m| m.end_time == window_end)
            .collect();

        let mut tokens = Vec::new();
        let mut conditions = Vec::new();

        for info in &window_markets {
            let gid = info.ids.condition_id.clone();

            let market_state = MarketState {
                info: info.clone(),
                binance_symbol: info.binance_symbol.clone(),
                start_time: info.start_time,
                end_time: info.end_time,
                binance_open_price: None,
            };

            tokens.push(info.ids.up_token.clone());
            tokens.push(info.ids.down_token.clone());

            // Build Optimization Map
            self.token_to_gamma
                .insert(info.ids.up_token.clone(), gid.clone());
            self.token_to_gamma
                .insert(info.ids.down_token.clone(), gid.clone());
            self.condition_to_gamma
                .insert(info.ids.condition_id.clone(), gid.clone());

            conditions.push(gid.clone());
            self.markets.insert(gid.clone(), market_state);
            self.state.insert(gid, ExecutionState::default());
        }

        self.current_window_end = Some(window_end);
        info!(
            "Discovered {} markets, window ends at {}",
            window_markets.len(),
            window_end
        );

        if !tokens.is_empty() {
            self.start_ws(tokens, conditions).await;
        }
    }

    async fn start_ws(&mut self, tokens: Vec<String>, conditions: Vec<String>) {
        let lookup = MarketLookup::new(&self.markets);

        let ob_client = self.ws_market.clone();
        let ob_tokens = tokens.clone();
        let price_tx = self.price_tx.clone();

        self.ws_tasks.push(tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match ob_client.subscribe_orderbook(ob_tokens.clone()) {
                    Ok(sub) => {
                        info!("Subscribed to orderbook");
                        backoff = Duration::from_millis(500);
                        let mut stream = Box::pin(sub);

                        while let Some(msg) = stream.next().await {
                            match msg {
                                Ok(BookUpdate {
                                    asset_id,
                                    bids,
                                    asks,
                                    ..
                                }) => {
                                    let bid = bids
                                        .iter()
                                        .max_by_key(|l| l.price)
                                        .and_then(|l| l.price.to_string().parse().ok());
                                    let ask = asks
                                        .iter()
                                        .min_by_key(|l| l.price)
                                        .and_then(|l| l.price.to_string().parse().ok());

                                    if let (Some(b), Some(a)) = (bid, ask) {
                                        let _ = price_tx
                                            .send(PriceUpdate {
                                                token_id: asset_id,
                                                bid: b,
                                                ask: a,
                                            })
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    warn!("Orderbook error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => warn!("Orderbook subscribe failed: {}", e),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }));

        let user_client = self.ws_user.clone();
        let user_conds = conditions;
        let user_lookup = lookup;
        let order_tx = self.order_tx.clone();

        self.ws_tasks.push(tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match user_client.subscribe_user_events(user_conds.clone()) {
                    Ok(sub) => {
                        info!("Subscribed to user events");
                        backoff = Duration::from_millis(500);
                        let mut stream = Box::pin(sub);

                        while let Some(msg) = stream.next().await {
                            match msg {
                                Ok(WsMessage::Order(ref o)) => {
                                    let gamma_id = match user_lookup.resolve_condition(&o.market) {
                                        Some(id) => id.to_string(),
                                        None => continue,
                                    };

                                    let side = match user_lookup
                                        .get_ids(&gamma_id)
                                        .and_then(|ids| ids.side_for_token(&o.asset_id))
                                    {
                                        Some(s) => s,
                                        None => continue,
                                    };

                                    let price = match o.price.to_string().parse() {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };

                                    let _ = order_tx
                                        .send(OrderEvent {
                                            order_id: o.id.clone(),
                                            gamma_id,
                                            token_id: o.asset_id.clone(),
                                            side,
                                            price,
                                            size_matched: Decimal::from(
                                                o.size_matched.unwrap_or_default(),
                                            ),
                                            msg_type: o.msg_type.clone().unwrap_or_default(),
                                        })
                                        .await;
                                }
                                Ok(WsMessage::Trade(ref t)) => {
                                    if t.status != "MATCHED" {
                                        continue;
                                    }

                                    let gamma_id = match user_lookup.resolve_condition(&t.market) {
                                        Some(id) => id.to_string(),
                                        None => continue,
                                    };

                                    use polymarket_client_sdk::clob::types::TraderSide;

                                    if t.trader_side == Some(TraderSide::Taker) {
                                        continue;
                                    }

                                    for mo in &t.maker_orders {
                                        let side = match user_lookup
                                            .get_ids(&gamma_id)
                                            .and_then(|ids| ids.side_for_token(&mo.asset_id))
                                        {
                                            Some(s) => s,
                                            None => continue,
                                        };

                                        let price: Decimal = match mo.price.to_string().parse() {
                                            Ok(p) => p,
                                            Err(_) => continue,
                                        };

                                        let matched: Decimal =
                                            match mo.matched_amount.to_string().parse() {
                                                Ok(m) => m,
                                                Err(_) => continue,
                                            };

                                        let _ = order_tx
                                            .send(OrderEvent {
                                                order_id: mo.order_id.clone(),
                                                gamma_id: gamma_id.clone(),
                                                token_id: mo.asset_id.clone(),
                                                side,
                                                price,
                                                size_matched: matched,
                                                msg_type: "TRADE_FILL".to_string(),
                                            })
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    warn!("User events error: {}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => warn!("User events subscribe failed: {}", e),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }));

        info!("Started WS for {} tokens", tokens.len());
    }

    async fn check_rollover(&mut self) {
        let expired = match self.current_window_end {
            Some(end) => Utc::now() >= end,
            None => true,
        };

        if expired {
            info!("Market window expired, refreshing");
            let _ = self.client.cancel_all_orders().await;
            self.discover_markets().await;
        }
    }

    fn log_status(&self) {
        info!("=== STATUS: {} markets ===", self.markets.len());

        for (gid, market) in &self.markets {
            let asset = market.info.asset.to_uppercase();
            let st = match self.state.get(gid) {
                Some(s) => s,
                None => continue,
            };

            let inv = self.inventory_snapshot(gid);
            let pairs = inv.total_pairs();
            let pnl = if pairs > dec!(0) {
                let up_cost = inv.avg_price(Side::Up).unwrap_or(dec!(0)) * pairs;
                let down_cost = inv.avg_price(Side::Down).unwrap_or(dec!(0)) * pairs;
                pairs - (up_cost + down_cost)
            } else {
                dec!(0)
            };

            let up_avg = inv.avg_price(Side::Up).map(|p| p.round_dp(3));
            let down_avg = inv.avg_price(Side::Down).map(|p| p.round_dp(3));

            info!(
                "[{}] inv: {} up (avg {:?}) / {} down (avg {:?}) | imb: {} | pairs: {} | pnl: ${:.2}",
                asset,
                inv.up_shares.round_dp(2),
                up_avg,
                inv.down_shares.round_dp(2),
                down_avg,
                inv.imbalance().round_dp(2),
                pairs.round_dp(2),
                pnl
            );

            let up_ladder: Vec<Decimal> = st.quotes.up_orders.iter().map(|o| o.price).collect();
            let down_ladder: Vec<Decimal> = st.quotes.down_orders.iter().map(|o| o.price).collect();

            info!(
                "[{}] book: up {:?}/{:?} | down {:?}/{:?}",
                asset, st.up_bid, st.up_ask, st.down_bid, st.down_ask
            );
            info!(
                "[{}] ladders: up {:?} | down {:?} | taker_in_flight: {}",
                asset, up_ladder, down_ladder, st.taker_in_flight
            );
        }
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }
}
