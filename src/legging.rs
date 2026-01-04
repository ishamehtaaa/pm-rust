use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{
    MarketIds, MarketInfo, MarketInventory, MarketLookup, MarketQuotes, MarketState,
    OrderEvent, RestingOrder, SecondLegParams, Side,
};

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use futures_util::stream::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{OrderType, Side as ClobSide, SignatureType};
use polymarket_client_sdk::clob::ws::{self, BookUpdate, WsMessage};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn, debug};

type AuthenticatedClient = Client<Authenticated<Normal>>;

// Constants
const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const LADDER_INTERVAL: Duration = Duration::from_secs(5);
const NOTIFICATION_INTERVAL: Duration = Duration::from_secs(30);
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const PRICE_TICK: Decimal = dec!(0.01);
const MIN_ORDER_SIZE: Decimal = dec!(5.0);
const MIN_NOTIONAL: Decimal = dec!(1.00);
const IMBALANCE_THRESHOLD: Decimal = dec!(2.0);
const LEVELS_PER_SIDE: usize = 5;
const TAKER_COOLDOWN: Duration = Duration::from_secs(2);
const BALANCE_ERROR_COOLDOWN: Duration = Duration::from_secs(30);

fn clamp_price(price: Decimal) -> Decimal {
    price.round_dp(2).max(MIN_PRICE).min(MAX_PRICE)
}

fn round_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
}

fn floor_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero)
}

fn min_shares_for_notional(price: Decimal) -> Decimal {
    if price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (MIN_NOTIONAL / price).round_dp_with_strategy(0, rust_decimal::RoundingStrategy::AwayFromZero)
}

#[derive(Debug, Default)]
struct ExecutionState {
    taker_in_flight: bool,
    last_taker_time: Option<Instant>,
    balance_error: bool,
    // Note: inventory moved to Bot level for persistence
    quotes: MarketQuotes,
    up_bid: Option<Decimal>,
    up_ask: Option<Decimal>,
    down_bid: Option<Decimal>,
    down_ask: Option<Decimal>,
    // Track processed trade_ids to avoid double-counting fills (for notifications reconciliation)
    processed_trade_ids: HashSet<String>,
    // Track fills that arrived before order was added to quotes
    // Entries are (fill_amount, timestamp) - stale entries get cleaned up
    prefilled: HashMap<String, (Decimal, Instant)>,
}

impl ExecutionState {
    fn can_send_taker(&self) -> bool {
        if self.taker_in_flight {
            return false;
        }
        if let Some(t) = self.last_taker_time {
            if self.balance_error && t.elapsed() < BALANCE_ERROR_COOLDOWN {
                return false;
            }
            if t.elapsed() < TAKER_COOLDOWN {
                return false;
            }
        }
        true
    }

    fn ask_for_side(&self, side: Side) -> Option<Decimal> {
        match side {
            Side::Up => self.up_ask,
            Side::Down => self.down_ask,
        }
    }
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

pub struct LeggingBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    
    markets: HashMap<String, MarketState>,
    state: HashMap<String, ExecutionState>,
    // Persistent inventory tracking across market window rotations
    global_inventory: HashMap<String, MarketInventory>,
    
    order_rx: mpsc::Receiver<OrderEvent>,
    order_tx: mpsc::Sender<OrderEvent>,
    price_rx: mpsc::Receiver<PriceUpdate>,
    price_tx: mpsc::Sender<PriceUpdate>,
    taker_rx: mpsc::Receiver<TakerResult>,
    taker_tx: mpsc::Sender<TakerResult>,
    
    ws_market: ws::Client,
    ws_user: ws::Client<Authenticated<Normal>>,
    ws_tasks: Vec<tokio::task::JoinHandle<()>>,
    
    current_window_end: Option<DateTime<Utc>>,
}

impl LeggingBot {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing Legging Bot");

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
        let ws_user = ws::Client::default().authenticate(creds, trader)?;

        let (order_tx, order_rx) = mpsc::channel(256);
        let (price_tx, price_rx) = mpsc::channel(1024);
        let (taker_tx, taker_rx) = mpsc::channel(64);

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            state: HashMap::new(),
            global_inventory: HashMap::new(),
            order_rx,
            order_tx,
            price_rx,
            price_tx,
            taker_rx,
            taker_tx,
            ws_market,
            ws_user,
            ws_tasks: Vec::new(),
            current_window_end: None,
        })
    }

    fn asset_name(&self, gamma_id: &str) -> String {
        self.markets
            .get(gamma_id)
            .map(|m| m.info.asset.to_uppercase())
            .unwrap_or_else(|| gamma_id.to_string())
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;

        let mut status_tick = tokio::time::interval(STATUS_INTERVAL);
        let mut ladder_tick = tokio::time::interval(LADDER_INTERVAL);
        let mut rollover_tick = tokio::time::interval(Duration::from_secs(1));
        // let mut notification_tick = tokio::time::interval(NOTIFICATION_INTERVAL);

        loop {
            tokio::select! {
                Some(ev) = self.order_rx.recv() => self.on_order_event(ev).await,
                Some(res) = self.taker_rx.recv() => self.on_taker_result(res),
                Some(upd) = self.price_rx.recv() => self.on_price_update(upd).await,
                _ = ladder_tick.tick() => self.maintain_all_ladders().await,
                _ = rollover_tick.tick() => self.check_rollover().await,
                // _ = notification_tick.tick() => self.reconcile_from_notifications().await,
                _ = status_tick.tick() => self.log_status(),
            }
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Notifications / Reconciliation
    // ───────────────────────────────────────────────────────────────────────────

    async fn reconcile_from_notifications(&mut self) {
        let notifications = match self.client.notifications().await {
            Ok(n) => n,
            Err(e) => {
                warn!("Failed to fetch notifications: {}", e);
                return;
            }
        };

        if notifications.is_empty() {
            return;
        }

        let lookup = MarketLookup::new(&self.markets);

        for notif in notifications {
            let payload = &notif.payload;
            
            let gamma_id = match lookup.resolve_condition(&payload.condition_id) {
                Some(id) => id.to_string(),
                None => continue,
            };

            let asset = self.asset_name(&gamma_id);

            let st = match self.state.get_mut(&gamma_id) {
                Some(s) => s,
                None => continue,
            };

            if st.processed_trade_ids.contains(&payload.trade_id) {
                continue;
            }

            let side = match lookup.get_ids(&gamma_id)
                .and_then(|ids| ids.side_for_token(&payload.asset_id))
            {
                Some(s) => s,
                None => continue,
            };

            let fill_size = payload.matched_size;
            if fill_size <= Decimal::ZERO {
                continue;
            }

            let price: Decimal = payload.price;

            let already_tracked = st.quotes
                .find_order_by_id(&payload.order_id)
                .is_some();

            if !already_tracked {
                info!(
                    "[{}] Reconciliation: adding {} {:?} @ {} (trade {} for order {})",
                    asset, fill_size, side, price, payload.trade_id, payload.order_id
                );
                let inv = self.global_inventory.entry(gamma_id.clone()).or_default();
                inv.add_buy(side, fill_size, price);
            }

            st.processed_trade_ids.insert(payload.trade_id.clone());
        }
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Event handlers
    // ───────────────────────────────────────────────────────────────────────────

    fn on_taker_result(&mut self, res: TakerResult) {
        if let Some(st) = self.state.get_mut(&res.gamma_id) {
            st.taker_in_flight = false;
            st.balance_error = res.balance_error;
        }

        if res.success && res.size > Decimal::ZERO {
            let inv = self.global_inventory.entry(res.gamma_id.clone()).or_default();
            inv.add_buy(res.side, res.size, res.price);
            info!("[{}] Taker fill confirmed: {} shares", self.asset_name(&res.gamma_id), res.size);
        }
    }


    async fn on_order_event(&mut self, ev: OrderEvent) {
        let gamma_id = ev.gamma_id.clone();
        let asset = self.asset_name(&gamma_id);

        // Step 1: Calculate the fill delta and extract second leg params without holding a long borrow
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

                    // Check if we are tracking this order
                    if let Some((o, _)) = st.quotes.find_order_mut_by_id(&ev.order_id) {
                        if ev.size_matched > o.filled {
                            delta = ev.size_matched - o.filled;
                            o.filled = ev.size_matched;
                            
                            // If order is now finished, capture second leg params
                            if o.is_fully_filled() {
                                params = o.second_leg.clone();
                            }
                        }
                    } else {
                        // Handle pre-fills (fills that arrive before the order ID is registered)
                        let entry = st.prefilled.entry(ev.order_id.clone())
                            .or_insert((Decimal::ZERO, Instant::now()));
                        
                        if ev.size_matched > entry.0 {
                            delta = ev.size_matched - entry.0;
                            entry.0 = ev.size_matched;
                        }
                    }
                    
                    // If it's fully filled and we found it in quotes, clear it now
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
            let inv = self.global_inventory.entry(gamma_id.clone()).or_default();
            inv.add_buy(ev.side, fill_delta, ev.price);

            info!(
                "[{}] Fill: {} {:?} @ {} | Net Inv: {}/{}",
                asset, fill_delta, ev.side, ev.price,
                inv.up_shares.round_dp(2), inv.down_shares.round_dp(2)
            );

            if let Some(params) = second_leg_params {
                self.maybe_second_leg(&gamma_id, Some(params)).await;
            }
        }
    }
    async fn on_price_update(&mut self, upd: PriceUpdate) {
        let gamma_id = self.markets.iter()
            .find(|(_, m)| m.info.ids.up_token == upd.token_id || m.info.ids.down_token == upd.token_id)
            .map(|(id, _)| id.clone());

        let gamma_id = match gamma_id {
            Some(id) => id,
            None => return,
        };

        let is_up = self.markets.get(&gamma_id)
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

    // ───────────────────────────────────────────────────────────────────────────
    // Taker logic
    // ───────────────────────────────────────────────────────────────────────────

    async fn maybe_rebalance(&mut self, gamma_id: &str) {
        let cfg = &self.config.legging_config;
        let inv = self.global_inventory.get(gamma_id).cloned().unwrap_or_default();

        let params = {
            let st = match self.state.get(gamma_id) {
                Some(s) => s,
                None => return,
            };

            if !st.can_send_taker() {
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
            let resting: Decimal = st.quotes.side_orders(needed).iter().map(|o| o.remaining()).sum();
            let capacity = cfg.target_shares_per_market - (current_shares + resting);
            
            if capacity < MIN_ORDER_SIZE {
                return;
            }

            let size = floor_size(imb.abs().min(capacity)).max(min_shares_for_notional(ask));
            if size < MIN_ORDER_SIZE || size > capacity {
                return;
            }

            Some((needed, size, clamp_price(ask + PRICE_TICK)))
        };

        if let Some((side, size, price)) = params {
            self.send_taker(gamma_id, side, size, price).await;
        }
    }

    async fn maybe_second_leg(&mut self, gamma_id: &str, precomputed: Option<SecondLegParams>) {
        let cfg = &self.config.legging_config;
        let inv = self.global_inventory.get(gamma_id).cloned().unwrap_or_default();

        let params = {
            let st = match self.state.get(gamma_id) {
                Some(s) => s,
                None => return,
            };

            if !st.can_send_taker() {
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

            let taker_price = clamp_price(ask + PRICE_TICK);
            if first_avg + taker_price > cfg.target_combined {
                return;
            }
            if dec!(1.0) - (first_avg + taker_price) < dec!(0.005) {
                return;
            }
            let current_shares = match needed {
                Side::Up => inv.up_shares,
                Side::Down => inv.down_shares,
            };

            let resting: Decimal = st.quotes.side_orders(needed).iter().map(|o| o.remaining()).sum();
            let capacity = cfg.target_shares_per_market - (current_shares + resting);

            if capacity < MIN_ORDER_SIZE {
                return;
            }

            let size = floor_size(imb.abs().min(capacity)).max(min_shares_for_notional(taker_price));
            if size < MIN_ORDER_SIZE || size > capacity {
                return;
            }

            Some((needed, size, taker_price))
        };

        if let Some((side, size, price)) = params {
            self.send_taker(gamma_id, side, size, price).await;
        }
    }

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

            st.quotes.side_orders_mut(side).drain(..).map(|o| o.order_id).collect()
        };

        info!("[{}] Taker: {} {:?} @ {} (cancelling {} orders)", asset, size, side, price, cancel_ids.len());

        let client = self.client.clone();
        let signer = self.signer.clone();
        let taker_tx = self.taker_tx.clone();
        let gamma_id = gamma_id.to_string();
        let dry_run = self.config.dry_run;

        tokio::spawn(async move {
            for oid in cancel_ids {
                let _ = client.cancel_order(&oid).await;
            }
            if dry_run {
                info!("[{}] [DRY] taker {} {:?} @ {}", asset, size, side, price);
                let _ = taker_tx.send(TakerResult {
                    gamma_id,
                    side,
                    size,
                    price,
                    success: true,
                    balance_error: false,
                }).await;
                return;
            }

            let size_p = match PolyDecimal::try_from(size.to_string().as_str()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to convert size: {}", asset, e);
                    let _ = taker_tx.send(TakerResult { gamma_id, side, size, price, success: false, balance_error: false }).await;
                    return;
                }
            };
            let price_p = match PolyDecimal::try_from(price.to_string().as_str()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to convert price: {}", asset, e);
                    let _ = taker_tx.send(TakerResult { gamma_id, side, size, price, success: false, balance_error: false }).await;
                    return;
                }
            };

            let order = match client.limit_order()
                .token_id(&token_id)
                .price(price_p)
                .size(size_p)
                .side(ClobSide::Buy)
                .order_type(OrderType::FOK)
                .build()
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to build order: {}", asset, e);
                    let _ = taker_tx.send(TakerResult { gamma_id, side, size, price, success: false, balance_error: false }).await;
                    return;
                }
            };

            let signed = match client.sign(&signer, order).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to sign: {}", asset, e);
                    let _ = taker_tx.send(TakerResult { gamma_id, side, size, price, success: false, balance_error: false }).await;
                    return;
                }
            };


            match client.post_order(signed).await {
                Ok(r) => {
                    // Use taking_amount for Taker orders (FOK/IOC)
                    let matched = r.taking_amount;
                    info!("[{}] Taker filled: {}/{} {:?} @ {} ({})", asset, matched, size, side, price, r.order_id);
                    let _ = taker_tx.send(TakerResult {
                        gamma_id,
                        side,
                        size: matched,
                        price,
                        success: true,
                        balance_error: false,
                    }).await;
                }
                Err(e) => {
                    let err = e.to_string();
                    let bal_err = err.contains("balance") || err.contains("allowance");
                    warn!("[{}] Taker rejected: {}", asset, err);
                    let _ = taker_tx.send(TakerResult { gamma_id, side, size, price, success: false, balance_error: bal_err }).await;
                }
            }
        });
    
    }

    // ───────────────────────────────────────────────────────────────────────────
    // Ladder (maker) logic
    // ───────────────────────────────────────────────────────────────────────────

    async fn maintain_all_ladders(&mut self) {
        let ids: Vec<String> = self.markets.keys().cloned().collect();
        for id in ids {
            self.maintain_ladder(&id).await;
        }
        
        // Clean up stale prefilled entries (from other people's orders we never claimed)
        const PREFILLED_TTL: Duration = Duration::from_secs(5);
        for st in self.state.values_mut() {
            st.prefilled.retain(|_order_id, (_, created)| created.elapsed() < PREFILLED_TTL);
        }
    }

    async fn maintain_ladder(&mut self, gamma_id: &str) {
        let cfg = &self.config.legging_config;
        let target_shares = cfg.target_shares_per_market;
        let target_combined = cfg.target_combined;

        let market = match self.markets.get(gamma_id) {
            Some(m) => m.info.clone(),
            None => return,
        };

        let inv = self.global_inventory.get(gamma_id).cloned().unwrap_or_default();

        let (_up_ask, _down_ask, up_target, down_target, up_cap, down_cap) = {
            let st = match self.state.get(gamma_id) {
                Some(s) => s,
                None => return,
            };

            let (up_ask, down_ask) = match (st.up_ask, st.down_ask) {
                (Some(u), Some(d)) => (u, d),
                _ => return,
            };

            let up_target = inv.avg_price(Side::Down)
                .map(|avg| target_combined - avg)
                .unwrap_or(target_combined - down_ask);

            let down_target = inv.avg_price(Side::Up)
                .map(|avg| target_combined - avg)
                .unwrap_or(target_combined - up_ask);

            let up_resting: Decimal = st.quotes.up_orders.iter().map(|o| o.remaining()).sum();
            let down_resting: Decimal = st.quotes.down_orders.iter().map(|o| o.remaining()).sum();

            let up_cap = target_shares - (inv.up_shares + up_resting);
            let down_cap = target_shares - (inv.down_shares + down_resting);

            (up_ask, down_ask, up_target, down_target, up_cap, down_cap)
        };

        let imb = inv.imbalance();

        if imb <= IMBALANCE_THRESHOLD && up_cap >= MIN_ORDER_SIZE {
            self.maintain_side_ladder(gamma_id, Side::Up, &market.ids, up_target, up_cap).await;
        } else {
            self.cancel_side(gamma_id, Side::Up).await;
        }

        if imb >= -IMBALANCE_THRESHOLD && down_cap >= MIN_ORDER_SIZE {
            self.maintain_side_ladder(gamma_id, Side::Down, &market.ids, down_target, down_cap).await;
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
        let base = clamp_price(base_price);

        if base < MIN_PRICE + PRICE_TICK || base > MAX_PRICE - PRICE_TICK {
            self.cancel_side(gamma_id, side).await;
            return;
        }

        let mut levels: Vec<Decimal> = Vec::new();
        let mut cap_left = capacity;
        for i in 0..LEVELS_PER_SIDE {
            let p = clamp_price(base - PRICE_TICK * Decimal::from(i as u32));
            if p < MIN_PRICE + PRICE_TICK || cap_left < MIN_ORDER_SIZE {
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

        for oid in &stale {
            let _ = self.client.cancel_order(oid).await;
        }

        if let Some(st) = self.state.get_mut(gamma_id) {
            *st.quotes.side_orders_mut(side) = keep.clone();
        }

        let covered: std::collections::HashSet<Decimal> = keep.iter()
            .map(|o| o.price.round_dp(2))
            .collect();

        let uncovered: Vec<Decimal> = levels.into_iter()
            .filter(|p| !covered.iter().any(|c| (*c - *p).abs() <= thresh))
            .collect();

        let second_side = match side { Side::Up => Side::Down, Side::Down => Side::Up };
        let token_id = ids.token_for_side(side);

        for price in uncovered {
            let size = cfg.shares_per_trade.max(min_shares_for_notional(price));
            let second_leg = SecondLegParams {
                token_id: ids.token_for_side(second_side).to_string(),
                side: second_side,
                max_price: cfg.target_combined - price,
            };

            if let Some(oid) = self.post_maker(token_id, price, size).await {
                let asset = self.asset_name(gamma_id);
                if let Some(st) = self.state.get_mut(gamma_id) {
                    let prefilled = st.prefilled.remove(&oid).map(|(amt, _)| amt).unwrap_or(Decimal::ZERO);
                    
                    if prefilled > Decimal::ZERO {
                        let inv = self.global_inventory.entry(gamma_id.to_string()).or_default();
                        inv.add_buy(side, prefilled, price);
                        info!(
                            "[{}] Fill (early): {} {:?} @ {} | order {} | inv: {}/{}",
                            asset, prefilled, side, price, oid,
                            inv.up_shares.round_dp(2), inv.down_shares.round_dp(2)
                        );
                    }
                    
                    st.quotes.side_orders_mut(side).push(RestingOrder {
                        order_id: oid.clone(),
                        token_id: token_id.to_string(),
                        price,
                        size,
                        filled: prefilled,
                        posted_at: Instant::now(),
                        second_leg: Some(second_leg),
                    });
                }
            }
        }
    }

    async fn cancel_side(&mut self, gamma_id: &str, side: Side) {
        let ids: Vec<String> = {
            let st = match self.state.get_mut(gamma_id) {
                Some(s) => s,
                None => return,
            };
            st.quotes.side_orders_mut(side).drain(..).map(|o| o.order_id).collect()
        };

        for oid in ids {
            let _ = self.client.cancel_order(&oid).await;
        }
    }

    async fn post_maker(&self, token_id: &str, price: Decimal, size: Decimal) -> Option<String> {
        if self.config.dry_run {
            return Some(format!("dry-{}", Utc::now().timestamp_millis()));
        }

        let size_p = PolyDecimal::try_from(round_size(size).to_string().as_str()).ok()?;
        let price_p = PolyDecimal::try_from(price.to_string().as_str()).ok()?;

        let order = self.client.limit_order()
            .token_id(token_id)
            .price(price_p)
            .size(size_p)
            .side(ClobSide::Buy)
            .order_type(OrderType::GTC)
            .build()
            .await.ok()?;

        let signed = self.client.sign(&self.signer, order).await.ok()?;
        self.client.post_order(signed).await.ok().map(|r| r.order_id)
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
        // Persistent inventory is NOT cleared
        self.state.clear();

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

            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            let market_state = MarketState {
                pair,
                info: info.clone(),
                binance_symbol: info.binance_symbol.clone(),
                start_time: info.start_time,
                end_time: info.end_time,
                binance_open_price: None,
            };

            tokens.push(info.ids.up_token.clone());
            tokens.push(info.ids.down_token.clone());
            conditions.push(gid.clone());

            self.markets.insert(gid.clone(), market_state);
            self.state.insert(gid, ExecutionState::default());
        }

        self.current_window_end = Some(window_end);
        info!("Discovered {} markets, window ends at {}", window_markets.len(), window_end);

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
                                Ok(BookUpdate { asset_id, bids, asks, .. }) => {
                                    let bid = bids.iter().max_by_key(|l| l.price)
                                        .and_then(|l| l.price.to_string().parse().ok());
                                    let ask = asks.iter().min_by_key(|l| l.price)
                                        .and_then(|l| l.price.to_string().parse().ok());

                                    if let (Some(b), Some(a)) = (bid, ask) {
                                        let _ = price_tx.send(PriceUpdate {
                                            token_id: asset_id,
                                            bid: b,
                                            ask: a,
                                        }).await;
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

                                    let side = match user_lookup.get_ids(&gamma_id)
                                        .and_then(|ids| ids.side_for_token(&o.asset_id))
                                    {
                                        Some(s) => s,
                                        None => continue,
                                    };

                                    let price = match o.price.to_string().parse() {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };

                                    let _ = order_tx.send(OrderEvent {
                                        order_id: o.id.clone(),
                                        gamma_id,
                                        token_id: o.asset_id.clone(),
                                        side,
                                        price,
                                        size_matched: Decimal::from(o.size_matched.unwrap_or_default()),
                                        msg_type: o.msg_type.clone().unwrap_or_default(),
                                    }).await;
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
                                        let side = match user_lookup.get_ids(&gamma_id)
                                            .and_then(|ids| ids.side_for_token(&mo.asset_id))
                                        {
                                            Some(s) => s,
                                            None => continue,
                                        };

                                        let price: Decimal = match mo.price.to_string().parse() {
                                            Ok(p) => p,
                                            Err(_) => continue,
                                        };

                                        let matched: Decimal = match mo.matched_amount.to_string().parse() {
                                            Ok(m) => m,
                                            Err(_) => continue,
                                        };

                                        let _ = order_tx.send(OrderEvent {
                                            order_id: mo.order_id.clone(),
                                            gamma_id: gamma_id.clone(),
                                            token_id: mo.asset_id.clone(),
                                            side,
                                            price,
                                            size_matched: matched,
                                            msg_type: "TRADE_FILL".to_string(),
                                        }).await;
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

            let inv = self.global_inventory.get(gid).cloned().unwrap_or_default();
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
                asset, inv.up_shares.round_dp(2), up_avg,
                inv.down_shares.round_dp(2), down_avg,
                inv.imbalance().round_dp(2), pairs.round_dp(2), pnl
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