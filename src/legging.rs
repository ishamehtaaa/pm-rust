use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{FillEvent, MarketIds, MarketLookup, MarketState, OrderEvent, Side};
use crate::ws::{PriceUpdate, spawn_orderbook_task, spawn_user_events_task};

use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{OrderType, Side as ClobSide, SignatureType, SignedOrder};
use polymarket_client_sdk::clob::ws as clob_ws;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const TARGET_COMBINED: Decimal = dec!(0.90);
const PRICE_TICK: Decimal = dec!(0.01);
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const ORDER_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const PAIR_SPREAD_LEVELS: usize = 3;
const MAX_BATCH_PAIRS: usize = 3;

#[derive(Debug, Clone)]
struct OrderPair {
    up_order_id: Option<String>,
    down_order_id: Option<String>,
    up_price: Decimal,
    down_price: Decimal,
    size: Decimal,
}

#[derive(Clone)]
struct PendingOrder {
    pair_index: usize,
    side: Side,
    token_id: String,
    price: Decimal,
    size: Decimal,
}

struct PlacementResult {
    pair_index: usize,
    side: Side,
    order_id: Option<String>,
}

#[derive(Debug)]
struct MarketPosition {
    up_shares: Decimal,
    down_shares: Decimal,
    up_cost: Decimal,
    down_cost: Decimal,
    order_pairs: Vec<OrderPair>,
    up_bid: Option<Decimal>,
    up_ask: Option<Decimal>,
    down_bid: Option<Decimal>,
    down_ask: Option<Decimal>,
    processed_fills: HashSet<String>,
    last_order_update: Instant,
}

impl Default for MarketPosition {
    fn default() -> Self {
        Self {
            up_shares: Decimal::ZERO,
            down_shares: Decimal::ZERO,
            up_cost: Decimal::ZERO,
            down_cost: Decimal::ZERO,
            order_pairs: Vec::new(),
            up_bid: None,
            up_ask: None,
            down_bid: None,
            down_ask: None,
            processed_fills: HashSet::new(),
            last_order_update: Instant::now(),
        }
    }
}

impl MarketPosition {
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

pub struct LeggingBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    markets: HashMap<String, MarketState>,
    positions: HashMap<String, MarketPosition>,

    // Channels
    order_rx: mpsc::Receiver<OrderEvent>,
    order_tx: mpsc::Sender<OrderEvent>,
    fill_rx: mpsc::Receiver<FillEvent>,
    fill_tx: mpsc::Sender<FillEvent>,
    price_rx: mpsc::Receiver<PriceUpdate>,
    price_tx: mpsc::Sender<PriceUpdate>,

    ws_market: clob_ws::Client,
    ws_user: clob_ws::Client<Authenticated<Normal>>,
    ws_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl LeggingBot {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing batch FOK Legging Bot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)?;
        let signer = signer.with_chain_id(Some(POLYGON));

        let (sig_type, funder, trader_addr) = if config.polymarket_proxy_address.trim().is_empty() {
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
        let ws_market = clob_ws::Client::default();
        let ws_user = clob_ws::Client::default().authenticate(creds, trader_addr)?;

        let (order_tx, order_rx) = mpsc::channel(256);
        let (fill_tx, fill_rx) = mpsc::channel(256);
        let (price_tx, price_rx) = mpsc::channel(1024);

        Ok(Self {
            config,
            client,
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
            ws_market,
            ws_user,
            ws_tasks: Vec::new(),
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;

        let mut order_tick = tokio::time::interval(ORDER_REFRESH_INTERVAL);
        let mut status_tick = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                Some(ev) = self.order_rx.recv() => self.handle_order_event(ev),
                Some(fill) = self.fill_rx.recv() => self.handle_fill(fill),
                Some(upd) = self.price_rx.recv() => self.handle_price_update(upd),
                _ = order_tick.tick() => self.refresh_all_orders().await,
                _ = status_tick.tick() => self.log_status(),
            }
        }
    }

    fn handle_fill(&mut self, fill: FillEvent) {
        let pos = match self.positions.get_mut(&fill.gamma_id) {
            Some(p) => p,
            None => return,
        };

        let key = format!("{}:{}", fill.trade_id, fill.order_id);
        if !pos.processed_fills.insert(key) {
            return;
        }

        match fill.side {
            Side::Up => {
                pos.up_shares += fill.size;
                pos.up_cost += fill.size * fill.price;
            }
            Side::Down => {
                pos.down_shares += fill.size;
                pos.down_cost += fill.size * fill.price;
            }
        }

        let asset = self
            .markets
            .get(&fill.gamma_id)
            .map(|m| m.info.asset.to_uppercase())
            .unwrap_or_default();

        info!(
            "[{}] FILL: {} {} @ {} | Total: {} UP, {} DOWN",
            asset,
            fill.size,
            if fill.side == Side::Up { "UP" } else { "DOWN" },
            fill.price,
            pos.up_shares,
            pos.down_shares
        );
    }

    fn handle_order_event(&mut self, ev: OrderEvent) {
        if ev.msg_type == "CANCELLATION" {
            if let Some(pos) = self.positions.get_mut(&ev.gamma_id) {
                for pair in &mut pos.order_pairs {
                    if pair.up_order_id.as_ref() == Some(&ev.order_id) {
                        pair.up_order_id = None;
                    }
                    if pair.down_order_id.as_ref() == Some(&ev.order_id) {
                        pair.down_order_id = None;
                    }
                }
            }
        }
    }

    fn handle_price_update(&mut self, upd: PriceUpdate) {
        for (gamma_id, market) in &self.markets {
            let ids = &market.info.ids;
            if let Some(pos) = self.positions.get_mut(gamma_id) {
                if upd.token_id == ids.up_token {
                    pos.up_bid = Some(upd.bid);
                    pos.up_ask = Some(upd.ask);
                } else if upd.token_id == ids.down_token {
                    pos.down_bid = Some(upd.bid);
                    pos.down_ask = Some(upd.ask);
                }
            }
        }
    }

    async fn refresh_all_orders(&mut self) {
        let gamma_ids: Vec<String> = self.markets.keys().cloned().collect();
        for gamma_id in gamma_ids {
            self.refresh_market_orders(&gamma_id).await;
        }
    }

    async fn refresh_market_orders(&mut self, gamma_id: &str) {
        let market = match self.markets.get(gamma_id) {
            Some(m) => m,
            None => return,
        };

        let (up_ask, down_ask, last_update) = {
            let pos = match self.positions.get(gamma_id) {
                Some(p) => p,
                None => return,
            };
            match (pos.up_ask, pos.down_ask) {
                (Some(u), Some(d)) => (u, d, pos.last_order_update),
                _ => return,
            }
        };

        if last_update.elapsed() < Duration::from_secs(5) {
            return;
        }

        let old_pairs = {
            let pos = match self.positions.get_mut(gamma_id) {
                Some(p) => p,
                None => return,
            };
            std::mem::take(&mut pos.order_pairs)
        };

        for pair in old_pairs {
            if let Some(id) = pair.up_order_id {
                let _ = self.client.cancel_order(&id).await;
            }
            if let Some(id) = pair.down_order_id {
                let _ = self.client.cancel_order(&id).await;
            }
        }

        let mut new_pairs = self.generate_order_pairs(up_ask, down_ask);
        let placements = Self::pending_orders_from_pairs(&new_pairs, &market.info.ids);
        let placement_results = self.place_order_batch(placements).await;

        for result in placement_results {
            if let Some(pair) = new_pairs.get_mut(result.pair_index) {
                match result.side {
                    Side::Up => pair.up_order_id = result.order_id,
                    Side::Down => pair.down_order_id = result.order_id,
                }
            }
        }

        if let Some(pos) = self.positions.get_mut(gamma_id) {
            pos.order_pairs = new_pairs;
            pos.last_order_update = Instant::now();
        }
    }

    fn generate_order_pairs(&self, up_center: Decimal, down_center: Decimal) -> Vec<OrderPair> {
        let mut pairs = Vec::new();
        let cfg = &self.config.legging_config;
        let size = cfg.shares_per_trade;

        for i in 0..PAIR_SPREAD_LEVELS {
            let offset = PRICE_TICK * Decimal::from(i as i64);

            let up_price = (up_center - offset).max(MIN_PRICE);
            let down_price = TARGET_COMBINED - up_price;

            if down_price >= MIN_PRICE && down_price <= MAX_PRICE {
                pairs.push(OrderPair {
                    up_order_id: None,
                    down_order_id: None,
                    up_price,
                    down_price,
                    size,
                });
            }

            let down_price = (down_center - offset).max(MIN_PRICE);
            let up_price = TARGET_COMBINED - down_price;

            if up_price >= MIN_PRICE && up_price <= MAX_PRICE {
                pairs.push(OrderPair {
                    up_order_id: None,
                    down_order_id: None,
                    up_price,
                    down_price,
                    size,
                });
            }
        }

        pairs.sort_by_key(|p| {
            (
                (p.up_price * dec!(100)).round(),
                (p.down_price * dec!(100)).round(),
            )
        });
        pairs.dedup_by_key(|p| {
            (
                (p.up_price * dec!(100)).round(),
                (p.down_price * dec!(100)).round(),
            )
        });

        pairs.truncate(MAX_BATCH_PAIRS);
        pairs
    }

    fn pending_orders_from_pairs(pairs: &[OrderPair], ids: &MarketIds) -> Vec<PendingOrder> {
        let mut placements = Vec::with_capacity(pairs.len() * 2);
        for (idx, pair) in pairs.iter().enumerate() {
            placements.push(PendingOrder {
                pair_index: idx,
                side: Side::Up,
                token_id: ids.up_token.clone(),
                price: pair.up_price,
                size: pair.size,
            });
            placements.push(PendingOrder {
                pair_index: idx,
                side: Side::Down,
                token_id: ids.down_token.clone(),
                price: pair.down_price,
                size: pair.size,
            });
        }
        placements
    }

    async fn place_order_batch(&self, placements: Vec<PendingOrder>) -> Vec<PlacementResult> {
        let signed_pairs = self.build_signed_orders(&placements).await;
        if signed_pairs.is_empty() {
            return Vec::new();
        }

        let (signed_placements, signed_orders): (Vec<_>, Vec<_>) = signed_pairs.into_iter().unzip();

        match self.client.post_orders(signed_orders).await {
            Ok(responses) => signed_placements
                .into_iter()
                .zip(responses.into_iter())
                .map(|(placement, response)| {
                    if !response.success {
                        warn!(
                            "Order rejected ({}): {}",
                            response.order_id,
                            response.error_msg.unwrap_or_else(|| "unknown".to_string())
                        );
                    }
                    PlacementResult {
                        pair_index: placement.pair_index,
                        side: placement.side,
                        order_id: response.success.then(|| response.order_id.clone()),
                    }
                })
                .collect(),
            Err(err) => {
                warn!("Batch order post failed: {}", err);
                Vec::new()
            }
        }
    }

    async fn build_signed_orders(
        &self,
        placements: &[PendingOrder],
    ) -> Vec<(PendingOrder, SignedOrder)> {
        let mut signed = Vec::with_capacity(placements.len());
        for placement in placements {
            if let Some(order) = self.build_signed_order(placement).await {
                signed.push((placement.clone(), order));
            }
        }
        signed
    }

    async fn build_signed_order(&self, placement: &PendingOrder) -> Option<SignedOrder> {
        if placement.price <= Decimal::ZERO || placement.size <= Decimal::ZERO {
            warn!(
                "Skipping invalid order {} {:?} @ {} size {}",
                placement.pair_index, placement.side, placement.price, placement.size
            );
            return None;
        }

        let price_poly =
            PolyDecimal::try_from(placement.price.round_dp(2).to_string().as_str()).ok()?;
        let size_poly =
            PolyDecimal::try_from(placement.size.round_dp(0).to_string().as_str()).ok()?;

        let signable = self
            .client
            .limit_order()
            .token_id(&placement.token_id)
            .price(price_poly)
            .size(size_poly)
            .side(ClobSide::Buy)
            .order_type(OrderType::FOK)
            .build()
            .await
            .ok()?;

        self.client.sign(&self.signer, signable).await.ok()
    }

    pub async fn discover_markets(&mut self) {
        for task in self.ws_tasks.drain(..) {
            task.abort();
        }

        let active = match self.market_cache.get_markets(Utc::now()).await {
            Ok(markets) => markets,
            Err(err) => {
                error!("Failed to fetch markets: {}", err);
                return;
            }
        };

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

        for info in window {
            let gamma_id = info.ids.gamma_id.clone();
            self.markets.insert(
                gamma_id.clone(),
                MarketState {
                    pair: Arc::new(RwLock::new(info.to_trading_pair())),
                    info: info.clone(),
                    binance_symbol: info.binance_symbol.clone(),
                    start_time: info.start_time,
                    end_time: info.end_time,
                    binance_open_price: None,
                },
            );
            self.positions.entry(gamma_id.clone()).or_default();
            all_tokens.push(info.ids.up_token.clone());
            all_tokens.push(info.ids.down_token.clone());
            all_conditions.push(info.ids.condition_id.clone());
        }

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

    fn log_status(&self) {
        for (gamma_id, market) in &self.markets {
            if let Some(pos) = self.positions.get(gamma_id) {
                let asset = market.info.asset.to_uppercase();
                let up_avg = pos.up_avg_price().unwrap_or(Decimal::ZERO);
                let down_avg = pos.down_avg_price().unwrap_or(Decimal::ZERO);
                let combined = if pos.up_shares > Decimal::ZERO && pos.down_shares > Decimal::ZERO {
                    up_avg + down_avg
                } else {
                    Decimal::ZERO
                };

                info!(
                    "[{}] {} UP @ {:.2}, {} DOWN @ {:.2} | Combined: ${:.2} | {} active pairs",
                    asset,
                    pos.up_shares,
                    up_avg,
                    pos.down_shares,
                    down_avg,
                    combined,
                    pos.order_pairs.len()
                );
                debug!(
                    "[{}] Book: up {}/{}, down {}/{}",
                    asset,
                    pos.up_bid.unwrap_or_default(),
                    pos.up_ask.unwrap_or_default(),
                    pos.down_bid.unwrap_or_default(),
                    pos.down_ask.unwrap_or_default(),
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
