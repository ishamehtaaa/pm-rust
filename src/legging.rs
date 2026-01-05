use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{FillEvent, MarketLookup, MarketState, OrderEvent, Side};
use crate::ws::{PriceUpdate, spawn_orderbook_task, spawn_user_events_task};

use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{OrderType, Side as ClobSide, SignatureType};
use polymarket_client_sdk::clob::ws as clob_ws;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::data::types::{
    MarketFilter, request::PositionsRequest, response::Position as PolyPosition,
};
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

type AuthenticatedClient = ClobClient<Authenticated<Normal>>;

// --- Strategy Constants ---
const BASE_TARGET: Decimal = dec!(0.98);
const PRICE_TICK: Decimal = dec!(0.01);
const MIN_NOTIONAL: Decimal = dec!(1.00);
const MIN_ORDER_SIZE: Decimal = dec!(5.0);
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(500);
const POSITIONS_POLL_INTERVAL: Duration = Duration::from_secs(2);

// --- Skew Constants ---
const SKEW_SENSITIVITY: Decimal = dec!(0.0005);

fn clamp_price(price: Decimal) -> Decimal {
    price.round_dp(2).max(MIN_PRICE).min(MAX_PRICE)
}

fn min_shares_for_notional(price: Decimal) -> Decimal {
    if price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (MIN_NOTIONAL / price).round_dp_with_strategy(0, RoundingStrategy::AwayFromZero)
}

#[derive(Debug, Clone)]
struct Order {
    id: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    filled: Decimal,
}

#[derive(Debug)]
struct Position {
    up_shares: Decimal,
    down_shares: Decimal,
    up_cost: Decimal,
    down_cost: Decimal,
    orders: Vec<Order>,
    up_bid: Option<Decimal>,
    up_ask: Option<Decimal>,
    down_bid: Option<Decimal>,
    down_ask: Option<Decimal>,
    processed_fills: HashSet<String>,
}

impl Position {
    fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }
    fn up_avg_price(&self) -> Option<Decimal> {
        if self.up_shares > Decimal::ZERO {
            Some(self.up_cost / self.up_shares)
        } else {
            None
        }
    }
    fn down_avg_price(&self) -> Option<Decimal> {
        if self.down_shares > Decimal::ZERO {
            Some(self.down_cost / self.down_shares)
        } else {
            None
        }
    }
}

impl Default for Position {
    fn default() -> Self {
        Self {
            up_shares: Decimal::ZERO,
            down_shares: Decimal::ZERO,
            up_cost: Decimal::ZERO,
            down_cost: Decimal::ZERO,
            orders: Vec::new(),
            up_bid: None,
            up_ask: None,
            down_bid: None,
            down_ask: None,
            processed_fills: HashSet::new(),
        }
    }
}

enum MarketAction {
    CancelAll(Vec<String>),
    PlaceLadder(String, Side, Vec<(Decimal, Decimal)>),
}

pub struct LeggingBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    data_client: Arc<DataClient>, // Added for Positions API
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    markets: HashMap<String, MarketState>,
    positions: HashMap<String, Position>,

    // Channels
    order_rx: mpsc::Receiver<OrderEvent>,
    order_tx: mpsc::Sender<OrderEvent>,
    fill_rx: mpsc::Receiver<FillEvent>,
    fill_tx: mpsc::Sender<FillEvent>,
    price_rx: mpsc::Receiver<PriceUpdate>,
    price_tx: mpsc::Sender<PriceUpdate>,
    position_rx: mpsc::Receiver<Vec<PolyPosition>>,
    position_tx: mpsc::Sender<Vec<PolyPosition>>,

    // Background Tasks
    ws_market: clob_ws::Client,
    ws_user: clob_ws::Client<Authenticated<Normal>>,
    ws_tasks: Vec<JoinHandle<()>>,
    positions_task: Option<JoinHandle<()>>,

    trader_addr: Address,
}

impl LeggingBot {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing Laddered Bot with Async Polling");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)?
            .with_chain_id(Some(POLYGON));

        let (sig_type, funder, trader) = if config.polymarket_proxy_address.trim().is_empty() {
            let addr = Address::from_str(&signer.address().to_string())?;
            (SignatureType::Eoa, None, addr)
        } else {
            let addr = Address::from_str(config.polymarket_proxy_address.trim())?;
            (SignatureType::Proxy, Some(addr), addr)
        };

        let trader_addr = trader.clone();
        let key_client =
            ClobClient::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?;
        let creds = key_client.create_or_derive_api_key(&signer, None).await?;
        drop(key_client);
        let data_client = Arc::new(DataClient::default());

        let mut auth = ClobClient::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer)
            .credentials(creds.clone());

        if let Some(f) = funder {
            auth = auth.funder(f).signature_type(sig_type);
        }

        let client = Arc::new(auth.authenticate().await?);
        let ws_market = clob_ws::Client::default();
        let ws_user = clob_ws::Client::default().authenticate(creds, trader)?;

        let (order_tx, order_rx) = mpsc::channel(256);
        let (fill_tx, fill_rx) = mpsc::channel(256);
        let (price_tx, price_rx) = mpsc::channel(1024);
        let (position_tx, position_rx) = mpsc::channel(64);

        Ok(Self {
            config,
            client,
            data_client,
            signer,
            market_cache,
            markets: HashMap::new(),
            positions: HashMap::new(),
            order_rx,
            order_tx,
            fill_rx,
            fill_tx,
            price_rx,
            price_tx,
            position_rx,
            position_tx,
            ws_market,
            ws_user,
            ws_tasks: Vec::new(),
            positions_task: None,
            trader_addr,
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;

        let mut maintain_tick = tokio::time::interval(MAINTENANCE_INTERVAL);
        let mut status_tick = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                Some(ev) = self.order_rx.recv() => self.handle_order_event(ev),
                Some(fill) = self.fill_rx.recv() => self.handle_fill(fill).await,
                Some(upd) = self.price_rx.recv() => self.handle_price_update(upd),
                Some(positions) = self.position_rx.recv() => self.handle_positions_update(positions),
                _ = maintain_tick.tick() => self.maintain_all_markets().await,
                _ = status_tick.tick() => self.log_status(),
            }
        }
    }

    fn asset_name(&self, gamma_id: &str) -> String {
        self.markets
            .get(gamma_id)
            .map(|m| m.info.asset.to_uppercase())
            .unwrap_or_else(|| gamma_id.to_string())
    }

    fn start_positions_polling(&mut self, conditions: Vec<String>) {
        if let Some(handle) = self.positions_task.take() {
            handle.abort();
        }
        if conditions.is_empty() {
            return;
        }

        let client = self.data_client.clone();
        let tx = self.position_tx.clone();
        let user = self.trader_addr.clone();

        self.positions_task = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(POSITIONS_POLL_INTERVAL);
            loop {
                interval.tick().await;
                let request = PositionsRequest::builder()
                    .user(user.clone())
                    .filter(MarketFilter::Markets(conditions.clone()))
                    .build();

                if let Ok(positions) = client.positions(&request).await {
                    if tx.send(positions).await.is_err() {
                        break;
                    }
                }
            }
        }));
    }

    fn handle_positions_update(&mut self, positions: Vec<PolyPosition>) {
        for (gamma_id, pos) in self.positions.iter_mut() {
            let market = match self.markets.get(gamma_id) {
                Some(m) => m,
                None => continue,
            };

            pos.up_shares = positions
                .iter()
                .find(|p| p.asset == market.info.ids.up_token)
                .map(|p| p.size)
                .unwrap_or(Decimal::ZERO);

            pos.down_shares = positions
                .iter()
                .find(|p| p.asset == market.info.ids.down_token)
                .map(|p| p.size)
                .unwrap_or(Decimal::ZERO);
        }
    }

    async fn maintain_market(&mut self, gamma_id: &str) {
        let mut actions = Vec::new();
        {
            let market = self.markets.get(gamma_id).unwrap();
            let pos = self.positions.get(gamma_id).unwrap();
            let imb = pos.imbalance();
            let asset = market.info.asset.to_uppercase();

            for side in [Side::Up, Side::Down] {
                let current: Vec<_> = pos.orders.iter().filter(|o| o.side == side).collect();
                let ladder = self.calculate_ladder(pos, side, imb);

                let price_mismatch = !current.is_empty()
                    && !ladder.is_empty()
                    && (current[0].price - ladder[0].0).abs() >= PRICE_TICK;
                let needs_refresh =
                    current.is_empty() || current.len() != ladder.len() || price_mismatch;

                if needs_refresh {
                    info!(
                        "[{}] Ladder refresh ({:?} side): {} existing orders, {} new rungs",
                        asset,
                        side,
                        current.len(),
                        ladder.len()
                    );
                    if !current.is_empty() {
                        actions.push(MarketAction::CancelAll(
                            current.iter().map(|o| o.id.clone()).collect(),
                        ));
                    }
                    if !ladder.is_empty() {
                        actions.push(MarketAction::PlaceLadder(
                            market.info.ids.token_for_side(side).to_string(),
                            side,
                            ladder,
                        ));
                    }
                }
            }
        }

        for action in actions {
            match action {
                MarketAction::CancelAll(ids) => {
                    for id in ids {
                        let _ = self.client.cancel_order(&id).await;
                    }
                }
                MarketAction::PlaceLadder(token, side, rungs) => {
                    for (price, size) in rungs {
                        self.execute_single_placement(gamma_id, token.clone(), side, price, size)
                            .await;
                    }
                }
            }
        }
    }

    fn calculate_ladder(
        &self,
        pos: &Position,
        side: Side,
        imb: Decimal,
    ) -> Vec<(Decimal, Decimal)> {
        let cfg = &self.config.legging_config;
        if cfg.max_levels == 0 {
            return Vec::new();
        }

        let anchor = match self.ladder_anchor(pos, side, imb) {
            Some(price) => price,
            None => return Vec::new(),
        };

        let mut remaining_capacity = self.remaining_capacity(pos, side);
        if remaining_capacity < MIN_ORDER_SIZE {
            return Vec::new();
        }

        let total_target = cfg
            .shares_per_trade
            .min(remaining_capacity.max(Decimal::ZERO));
        if total_target <= Decimal::ZERO {
            return Vec::new();
        }

        let level_count = Decimal::from(cfg.max_levels as u32);
        let base_size = (total_target / level_count).max(MIN_ORDER_SIZE);
        let mut rungs = Vec::new();

        for level in 0..cfg.max_levels {
            let price = self.rung_price(anchor, side, level as u32);
            let min_notional = min_shares_for_notional(price).max(MIN_ORDER_SIZE);

            if remaining_capacity < min_notional {
                break;
            }

            let desired = base_size.max(min_notional);
            let size = remaining_capacity.min(desired);

            if size < min_notional || size < MIN_ORDER_SIZE {
                break;
            }

            rungs.push((price, size));
            remaining_capacity = (remaining_capacity - size).max(Decimal::ZERO);
        }

        rungs
    }

    fn ladder_anchor(&self, pos: &Position, side: Side, imb: Decimal) -> Option<Decimal> {
        let observed = match side {
            Side::Up => pos.up_bid.or(pos.up_ask),
            Side::Down => pos.down_ask.or(pos.down_bid),
        };
        let other_basis = match side {
            Side::Up => pos.down_avg_price().or(pos.down_ask),
            Side::Down => pos.up_avg_price().or(pos.up_ask),
        };
        let fallback = other_basis.map(|b| BASE_TARGET - b);
        let base = observed.or(fallback)?;
        let skew = match side {
            Side::Up => -imb * SKEW_SENSITIVITY,
            Side::Down => imb * SKEW_SENSITIVITY,
        };
        Some(clamp_price(base + skew))
    }

    fn remaining_capacity(&self, pos: &Position, side: Side) -> Decimal {
        let cfg = &self.config.legging_config;
        let side_shares = match side {
            Side::Up => pos.up_shares,
            Side::Down => pos.down_shares,
        };
        let outstanding: Decimal = pos
            .orders
            .iter()
            .filter(|o| o.side == side)
            .map(|o| (o.size - o.filled).max(Decimal::ZERO))
            .sum();

        let available = cfg.max_shares_per_market - side_shares - outstanding;
        available.max(Decimal::ZERO)
    }

    fn rung_price(&self, anchor: Decimal, side: Side, step: u32) -> Decimal {
        let offset = PRICE_TICK * Decimal::from(step);
        match side {
            Side::Up => clamp_price(anchor - offset),
            Side::Down => clamp_price(anchor + offset),
        }
    }

    async fn execute_single_placement(
        &mut self,
        gamma_id: &str,
        token_id: String,
        side: Side,
        price: Decimal,
        size: Decimal,
    ) {
        if self.config.dry_run {
            return;
        }
        let asset = self.asset_name(gamma_id);
        info!(
            "[{}] Posting ladder order ({:?} side) @ {} size {}",
            asset, side, price, size
        );
        let order = self
            .client
            .limit_order()
            .token_id(&token_id)
            .price(PolyDecimal::try_from(price.to_string().as_str()).unwrap())
            .size(PolyDecimal::try_from(size.to_string().as_str()).unwrap())
            .side(ClobSide::Buy)
            .order_type(OrderType::GTC)
            .build()
            .await;

        if let Ok(o) = order {
            if let Ok(signed) = self.client.sign(&self.signer, o).await {
                if let Ok(resp) = self.client.post_order(signed).await {
                    if let Some(pos) = self.positions.get_mut(gamma_id) {
                        let order_id = resp.order_id.clone();
                        pos.orders.push(Order {
                            id: order_id.clone(),
                            side,
                            price,
                            size,
                            filled: Decimal::ZERO,
                        });
                        info!("[{}] Ladder order recorded (order_id={})", asset, order_id);
                    }
                }
            }
        }
    }

    fn handle_order_event(&mut self, ev: OrderEvent) {
        if let Some(pos) = self.positions.get_mut(&ev.gamma_id) {
            if ev.msg_type == "CANCELLATION" || ev.msg_type == "CLOSED" {
                pos.orders.retain(|o| o.id != ev.order_id);
            }
        }
    }

    async fn handle_fill(&mut self, fill: FillEvent) {
        if let Some(pos) = self.positions.get_mut(&fill.gamma_id) {
            let key = format!("{}:{}", fill.trade_id, fill.order_id);
            if !pos.processed_fills.insert(key) {
                return;
            }
            match fill.side {
                Side::Up => pos.up_cost += fill.size * fill.price,
                Side::Down => pos.down_cost += fill.size * fill.price,
            }
            for order in &mut pos.orders {
                if order.id == fill.order_id {
                    order.filled += fill.size;
                }
            }
            pos.orders.retain(|o| o.filled < o.size);
        }
    }

    fn handle_price_update(&mut self, upd: PriceUpdate) {
        for (gamma_id, market) in &self.markets {
            if let Some(pos) = self.positions.get_mut(gamma_id) {
                if upd.token_id == market.info.ids.up_token {
                    pos.up_bid = Some(upd.bid);
                    pos.up_ask = Some(upd.ask);
                } else if upd.token_id == market.info.ids.down_token {
                    pos.down_bid = Some(upd.bid);
                    pos.down_ask = Some(upd.ask);
                }
            }
        }
    }

    pub async fn discover_markets(&mut self) {
        for task in self.ws_tasks.drain(..) {
            task.abort();
        }
        let active = self
            .market_cache
            .get_markets(Utc::now())
            .await
            .unwrap_or_default();
        if active.is_empty() {
            return;
        }
        let earliest = active.iter().map(|m| m.end_time).min().unwrap();
        let window: Vec<_> = active
            .into_iter()
            .filter(|m| m.end_time == earliest)
            .collect();

        let mut all_tokens = Vec::new();
        let mut all_conditions = Vec::new();
        for m in window {
            let gid = m.ids.gamma_id.clone();
            self.markets.insert(
                gid.clone(),
                MarketState {
                    pair: Arc::new(RwLock::new(m.to_trading_pair())),
                    info: m.clone(),
                    binance_symbol: m.binance_symbol.clone(),
                    start_time: m.start_time,
                    end_time: m.end_time,
                    binance_open_price: None,
                },
            );
            self.positions.entry(gid).or_default();
            all_tokens.push(m.ids.up_token.clone());
            all_tokens.push(m.ids.down_token.clone());
            all_conditions.push(m.ids.condition_id.clone());
        }

        self.start_positions_polling(all_conditions.clone());

        if !all_tokens.is_empty() {
            let lookup = MarketLookup::new(&self.markets);
            self.ws_tasks.push(spawn_orderbook_task(
                self.ws_market.clone(),
                all_tokens,
                self.price_tx.clone(),
            ));
            self.ws_tasks.push(spawn_user_events_task(
                self.ws_user.clone(),
                all_conditions,
                lookup,
                self.order_tx.clone(),
                self.fill_tx.clone(),
                self.price_tx.clone(),
            ));
        }
    }

    async fn maintain_all_markets(&mut self) {
        let ids: Vec<String> = self.markets.keys().cloned().collect();
        for gid in ids {
            self.maintain_market(&gid).await;
        }
    }

    fn log_status(&self) {
        for (gid, m) in &self.markets {
            if let Some(p) = self.positions.get(gid) {
                info!(
                    "[{}] On-Chain: {} U | {} D | Imb: {} | Rungs: {}",
                    m.info.asset.to_uppercase(),
                    p.up_shares,
                    p.down_shares,
                    p.imbalance(),
                    p.orders.len()
                );
            }
        }
    }
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }
}
