use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use futures_util::stream::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::OrderType;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::ws::{self, BookUpdate, TradeMessage, WsMessage};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const STATUS_PRINT_INTERVAL: Duration = Duration::from_secs(10);
const QUOTE_UPDATE_INTERVAL: Duration = Duration::from_millis(500);
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const LIMIT_TICK_DP: u32 = 2;

// Don't post bids too close to the edges
const PRICE_BUFFER: Decimal = dec!(0.03);

// Minimum time between requotes on the same side
const MIN_REQUOTE_INTERVAL: Duration = Duration::from_secs(3);

// Polymarket minimum order size
const MIN_ORDER_SIZE: Decimal = dec!(5.0);

fn round_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
}

// For taker orders: round to whole number so price*size has max 2 decimals
fn round_size_for_taker(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero)
}

#[derive(Debug, Clone)]
pub struct OrderEvent {
    pub order_id: String,
    pub market_id: String,  // numeric ID
    pub token_id: String,
    pub side: LegSide,
    pub price: Decimal,
    pub size_matched: Decimal,
    pub msg_type: String,  // PLACEMENT, CANCELLATION, etc.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegSide {
    Up,
    Down,
}

impl LegSide {
    fn opposite(&self) -> LegSide {
        match self {
            LegSide::Up => LegSide::Down,
            LegSide::Down => LegSide::Up,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order_id: String,
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub filled: Decimal,
    pub posted_at: Instant,
}

impl RestingOrder {
    pub fn remaining(&self) -> Decimal {
        (self.size - self.filled).max(Decimal::ZERO)
    }

    pub fn is_fully_filled(&self) -> bool {
        self.filled >= self.size
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketQuotes {
    pub up_order: Option<RestingOrder>,
    pub down_order: Option<RestingOrder>,
}

impl MarketQuotes {
    pub fn find_order_mut(&mut self, order_id: &str) -> Option<(&mut RestingOrder, LegSide)> {
        if let Some(ref mut order) = self.up_order {
            if order.order_id == order_id {
                return Some((order, LegSide::Up));
            }
        }
        if let Some(ref mut order) = self.down_order {
            if order.order_id == order_id {
                return Some((order, LegSide::Down));
            }
        }
        None
    }

    pub fn clear_order(&mut self, side: LegSide) {
        match side {
            LegSide::Up => self.up_order = None,
            LegSide::Down => self.down_order = None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketInventory {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub up_cost_basis: Decimal,
    pub down_cost_basis: Decimal,
}

impl MarketInventory {
    pub fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }

    pub fn total_shares(&self) -> Decimal {
        self.up_shares.min(self.down_shares)
    }

    pub fn add_buy(&mut self, side: LegSide, size: Decimal, price: Decimal) {
        match side {
            LegSide::Up => {
                self.up_shares += size;
                self.up_cost_basis += price * size;
            }
            LegSide::Down => {
                self.down_shares += size;
                self.down_cost_basis += price * size;
            }
        }
    }
}

fn round_to_tick(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(LIMIT_TICK_DP, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
}

fn clamp_price(price: Decimal) -> Decimal {
    round_to_tick(price).max(MIN_PRICE).min(MAX_PRICE)
}

pub struct LeggingBot {
    pub config: Config,
    pub client: Arc<AuthenticatedClient>,
    pub signer_with_chain: PrivateKeySigner,
    pub trader_address: Address,
    pub signature_type: SignatureType,

    pub ws_market: ws::Client,
    pub ws_user: ws::Client<Authenticated<Normal>>,

    // token_id -> (bid, ask, timestamp_ms)
    pub ws_price_cache: Arc<RwLock<HashMap<String, (Decimal, Decimal, i64)>>>,
    // market_id -> inventory (updated by WS task)
    pub ws_inventory: Arc<RwLock<HashMap<String, MarketInventory>>>,
    // market_id -> quotes (our resting orders, updated in main loop only)
    pub market_quotes: HashMap<String, MarketQuotes>,

    pub market_cache: MarketCache,
    pub markets: HashMap<String, MarketState>,

    pub ws_tasks: Vec<tokio::task::JoinHandle<()>>,
    // Channel for receiving order events from WS task (ORDER messages)
    pub order_tx: mpsc::Sender<OrderEvent>,
    pub order_rx: mpsc::Receiver<OrderEvent>,
    pub current_window_end: Option<DateTime<Utc>>,
}

impl LeggingBot {
    #[instrument(skip(config))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing Maker-Based Legging Bot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)?;
        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));

        let (signature_type, funder, trader_address) =
            if config.polymarket_proxy_address.trim().is_empty() {
                let addr = Address::from_str(&signer.address().to_string())?;
                (SignatureType::Eoa, None, addr)
            } else {
                let addr = Address::from_str(config.polymarket_proxy_address.trim())?;
                (SignatureType::Proxy, Some(addr), addr)
            };

        let key_client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?;
        let credentials = key_client
            .create_or_derive_api_key(&signer_with_chain, None)
            .await?;
        drop(key_client);

        let mut auth = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer_with_chain)
            .credentials(credentials.clone());

        if let Some(addr) = funder {
            auth = auth.funder(addr).signature_type(SignatureType::Proxy);
        }

        let client = auth.authenticate().await?;
        let ws_market = ws::Client::default();
        let ws_user = ws::Client::default().authenticate(credentials, trader_address)?;

        // Channel for order events from WS task to main loop (ORDER messages)
        let (order_tx, order_rx) = mpsc::channel::<OrderEvent>(256);

        Ok(Self {
            config,
            client: Arc::new(client),
            signer_with_chain,
            trader_address,
            signature_type,
            ws_market,
            ws_user,
            ws_price_cache: Arc::new(RwLock::new(HashMap::new())),
            ws_inventory: Arc::new(RwLock::new(HashMap::new())),
            market_quotes: HashMap::new(),
            market_cache,
            markets: HashMap::new(),
            ws_tasks: Vec::new(),
            order_tx,
            order_rx,
            current_window_end: None,
        })
    }

    pub async fn run(&mut self) {
        let mut status_tick = tokio::time::interval(STATUS_PRINT_INTERVAL);
        let mut quote_tick = tokio::time::interval(QUOTE_UPDATE_INTERVAL);
        let mut rollover_tick = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                Some(order_event) = self.order_rx.recv() => {
                    self.handle_order_event(order_event).await;
                }
                _ = quote_tick.tick() => {
                    self.maintain_all_quotes().await;
                }
                _ = rollover_tick.tick() => {
                    self.check_market_rollover().await;
                }
                _ = status_tick.tick() => {
                    self.log_status();
                }
            }
        }
    }

    async fn check_market_rollover(&mut self) {
        let now = Utc::now();
        let window_expired = match self.current_window_end {
            Some(end) => now >= end,
            None => true,
        };

        if window_expired {
            info!("Market window expired at {}, refreshing", now);
            self.cancel_all_orders().await;
            self.discover_markets().await;
        }
    }

    fn log_status(&self) {
        let inv = self.ws_inventory.read();
        info!("=== Status: {} markets ===", self.markets.len());

        for (market_id, state) in &self.markets {
            let inventory = inv.get(market_id);
            let quotes = self.market_quotes.get(market_id);

            let (up_shares, down_shares) = inventory
                .map(|i| (i.up_shares, i.down_shares))
                .unwrap_or((dec!(0), dec!(0)));

            let (up_bid, down_bid) = quotes
                .map(|q| {
                    (
                        q.up_order.as_ref().map(|o| o.price),
                        q.down_order.as_ref().map(|o| o.price),
                    )
                })
                .unwrap_or((None, None));

            let cache = self.ws_price_cache.read();
            let up_ask = cache.get(&state.info.up_token_id).map(|(_, a, _)| *a);
            let down_ask = cache.get(&state.info.down_token_id).map(|(_, a, _)| *a);

            info!(
                "[{}] inv: {}↑ {}↓ | bids: {:?}↑ {:?}↓ | asks: {:?}↑ {:?}↓",
                state.info.asset,
                up_shares,
                down_shares,
                up_bid,
                down_bid,
                up_ask,
                down_ask
            );
        }
    }

    pub async fn discover_markets(&mut self) {
        let now = Utc::now();
        match self.market_cache.get_markets(now).await {
            Ok(active_markets) => {
                self.markets.clear();
                self.market_quotes.clear();

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

                for info in &window_markets {
                    let trading_pair = info.to_trading_pair();
                    let state = MarketState {
                        pair: Arc::new(RwLock::new(trading_pair)),
                        info: info.clone(),
                        binance_symbol: info.binance_symbol.clone(),
                        start_time: info.start_time,
                        end_time: info.end_time,
                        binance_open_price: None,
                    };
                    self.markets.insert(info.id.clone(), state);
                    self.market_quotes.insert(info.id.clone(), MarketQuotes::default());
                }

                // Clean up inventory for markets no longer active
                let active_ids: std::collections::HashSet<_> =
                    window_markets.iter().map(|m| m.id.clone()).collect();
                self.ws_inventory
                    .write()
                    .retain(|id, _| active_ids.contains(id));

                self.current_window_end = Some(window_end);
                info!(
                    "Discovered {} markets, window ends at {}",
                    window_markets.len(),
                    window_end
                );

                self.restart_ws_tasks();
            }
            Err(err) => {
                warn!("Failed to discover markets: {}", err);
            }
        }
    }

    fn restart_ws_tasks(&mut self) {
        for t in self.ws_tasks.drain(..) {
            t.abort();
        }

        let price_cache = self.ws_price_cache.clone();
        let order_tx = self.order_tx.clone();

        let asset_ids: Vec<String> = self
            .markets
            .values()
            .flat_map(|s| vec![s.info.up_token_id.clone(), s.info.down_token_id.clone()])
            .collect();

        // Use condition_id for WS user subscription (not the numeric id)
        let condition_ids: Vec<String> = self
            .markets
            .values()
            .map(|s| s.info.condition_id.clone())
            .collect();

        // Map condition_id -> (up_token, down_token) for trade processing
        // Also map condition_id -> numeric_id for inventory tracking
        let market_tokens: HashMap<String, (String, String)> = self
            .markets
            .values()
            .map(|v| (v.info.condition_id.clone(), (v.info.up_token_id.clone(), v.info.down_token_id.clone())))
            .collect();

        let condition_to_id: HashMap<String, String> = self
            .markets
            .values()
            .map(|v| (v.info.condition_id.clone(), v.info.id.clone()))
            .collect();

        info!(
            "Starting WS for {} tokens, {} markets",
            asset_ids.len(),
            condition_ids.len()
        );
        info!("Condition IDs for user subscription: {:?}", condition_ids);

        // Orderbook stream (public, no auth needed)
        let orderbook_client = self.ws_market.clone();
        let orderbook_ids = asset_ids.clone();
        let orderbook_cache = price_cache.clone();

        self.ws_tasks.push(tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);
            loop {
                match orderbook_client.subscribe_orderbook(orderbook_ids.clone()) {
                    Ok(sub) => {
                        info!("Subscribed to orderbook");
                        backoff = Duration::from_millis(500);
                        let mut stream = Box::pin(sub);

                        while let Some(msg) = stream.next().await {
                            match msg {
                                Ok(BookUpdate {
                                    asset_id,
                                    timestamp,
                                    bids,
                                    asks,
                                    ..
                                }) => {
                                    let best_bid = bids.iter().max_by_key(|l| l.price).map(|l| l.price);
                                    let best_ask = asks.iter().min_by_key(|l| l.price).map(|l| l.price);

                                    if let (Some(b), Some(a)) = (best_bid, best_ask) {
                                        if let (Ok(bid), Ok(ask)) = (
                                            b.to_string().parse::<Decimal>(),
                                            a.to_string().parse::<Decimal>(),
                                        ) {
                                            orderbook_cache.write().insert(asset_id, (bid, ask, timestamp));
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Orderbook error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Orderbook subscription failed: {}", e);
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }));

        // User trade stream (authenticated - YOUR fills only)
        let trade_client = self.ws_user.clone();
        let trade_condition_ids = condition_ids.clone();
        let trade_tokens = market_tokens.clone();
        let trade_condition_to_id = condition_to_id.clone();
        let trade_order_tx = order_tx.clone();

        info!("Will subscribe to USER events for condition_ids: {:?}", trade_condition_ids);

        self.ws_tasks.push(tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);

            loop {
                info!("Attempting to subscribe to USER events...");
                // Use subscribe_user_events to see ALL messages (orders + trades)
                match trade_client.subscribe_user_events(trade_condition_ids.clone()) {
                    Ok(sub) => {
                        info!("Subscribed to USER events for {} markets", trade_condition_ids.len());
                        backoff = Duration::from_millis(500);
                        let mut stream = Box::pin(sub);

                        while let Some(msg) = stream.next().await {
                            match msg {
                                Ok(ws_msg) => {
                                    // Extract trade if it's a trade message
                                    let trade = match ws_msg {
                                        WsMessage::Trade(t) => t,
                                        WsMessage::Order(o) => {
                                            // Process ORDER events
                                            let msg_type = o.msg_type.clone().unwrap_or_default();
                                            let size_matched = o.size_matched.unwrap_or_default();
                                            
                                            info!(
                                                "ORDER EVENT: type={} side={:?} price={} size_matched={}",
                                                 msg_type, o.side, o.price, size_matched
                                            );
                                            debug!("Order event for ID: {}", o.id);
                                            
                                            // Determine leg side from asset_id
                                            let leg_side = if let Some((up_token, down_token)) = trade_tokens.get(&o.market) {
                                                if o.asset_id == *up_token {
                                                    LegSide::Up
                                                } else if o.asset_id == *down_token {
                                                    LegSide::Down
                                                } else {
                                                    continue;
                                                }
                                            } else {
                                                continue;
                                            };
                                            
                                            // Convert condition_id to numeric ID
                                            let numeric_id = match trade_condition_to_id.get(&o.market) {
                                                Some(id) => id.clone(),
                                                None => continue,
                                            };
                                            
                                            // Parse price and size_matched
                                            let price_dec = match o.price.to_string().parse::<Decimal>() {
                                                Ok(v) => v,
                                                Err(_) => continue,
                                            };
                                            let size_matched_dec = Decimal::from(size_matched);
                                            
                                            let order_event = OrderEvent {
                                                order_id: o.id.clone(),
                                                market_id: numeric_id,
                                                token_id: o.asset_id.clone(),
                                                side: leg_side,
                                                price: price_dec,
                                                size_matched: size_matched_dec,
                                                msg_type,
                                            };
                                            
                                            if trade_order_tx.send(order_event).await.is_err() {
                                                warn!("Order event channel closed");
                                                return;
                                            }
                                            continue;
                                        }
                                        other => {
                                            debug!("OTHER EVENT: {:?}", other);
                                            continue;
                                        }
                                    };

                                    // Log TRADE messages for debugging only
                                    // Actual fill tracking is done via ORDER UPDATE
                                    debug!(
                                        "TRADE (info only): id={} side={:?} size={} price={} status={} trader_side={:?}",
                                        trade.id, trade.side, trade.size, trade.price,
                                        trade.status, trade.trader_side
                                    );
                                }
                                Err(e) => {
                                    warn!("Trade stream error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Trade subscription failed: {}", e);
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }));
    }

    async fn handle_order_event(&mut self, event: OrderEvent) {
        // Find the order in our quotes
        let quotes = match self.market_quotes.get_mut(&event.market_id) {
            Some(q) => q,
            None => return,  // Unknown market
        };

        // Find order by ID
        let (order, side) = match quotes.find_order_mut(&event.order_id) {
            Some(found) => found,
            None => return,  // Not our order or already cleared
        };

        match event.msg_type.as_str() {
            "PLACEMENT" => {
                debug!("Order {} confirmed on book", event.order_id);
            }
            "CANCELLATION" => {
                info!("Order {} was cancelled, clearing", event.order_id);
                quotes.clear_order(event.side);
            }
            "UPDATE" | _ => {
                // Check size_matched for fills
                let new_filled = event.size_matched;
                if new_filled > order.filled {
                    let fill_amount = new_filled - order.filled;
                    let market_id = event.market_id.clone();
                    let price = event.price;

                    info!(
                        "FILL via ORDER UPDATE: {} {:?} @ {} (total filled: {}/{})",
                        fill_amount, side, price, new_filled, order.size
                    );

                    // Update our tracked fill
                    order.filled = new_filled;

                    // Update inventory
                    {
                        let mut inv = self.ws_inventory.write();
                        let entry = inv.entry(market_id.clone()).or_default();
                        entry.add_buy(side, fill_amount, price);

                        info!(
                            "Inventory updated: {}↑ {}↓",
                            entry.up_shares, entry.down_shares
                        );
                    }

                    // If fully filled, clear the order
                    if order.is_fully_filled() {
                        info!("Order {} fully filled", event.order_id);
                        quotes.clear_order(side);
                    }

                    // Check if we need to complete the other leg
                    let imbalance = {
                        let inv = self.ws_inventory.read();
                        inv.get(&market_id)
                            .map(|i| i.imbalance())
                            .unwrap_or(dec!(0))
                    };

                    if imbalance.abs() >= dec!(1.0) {
                        info!(
                            "Imbalance {} in market {}, completing opposite leg",
                            imbalance, market_id
                        );
                        self.complete_second_leg(&market_id, imbalance).await;
                    }
                }
            }
        }
    }

    async fn complete_second_leg(&mut self, market_id: &str, imbalance: Decimal) {
        // Extract data upfront to avoid borrow conflicts
        let (up_token_id, down_token_id, taker_buffer) = {
            let state = match self.markets.get(market_id) {
                Some(s) => s,
                None => return,
            };
            (
                state.info.up_token_id.clone(),
                state.info.down_token_id.clone(),
                self.config.legging_config.taker_buffer,
            )
        };

        let (token_id, side) = if imbalance > dec!(0) {
            (down_token_id, LegSide::Down)
        } else {
            (up_token_id, LegSide::Up)
        };

        // Round size to whole number for taker orders (ensures price*size has max 2 decimals)
        let size = round_size_for_taker(imbalance.abs());

        // Skip if below minimum order size
        if size < MIN_ORDER_SIZE {
            debug!(
                "Imbalance {} rounds to {} which is below minimum {}, skipping second leg",
                imbalance, size, MIN_ORDER_SIZE
            );
            return;
        }

        let ask = {
            let cache = self.ws_price_cache.read();
            cache.get(&token_id).map(|(_, a, _)| *a)
        };

        let price = match ask {
            Some(a) => clamp_price(a + taker_buffer),
            None => {
                warn!("No ask price for {}, cannot complete leg", token_id);
                return;
            }
        };

        info!("Completing {:?} leg: {} @ {} (taker)", side, size, price);

        // Cancel our resting order on this side first
        if let Some(quotes) = self.market_quotes.get_mut(market_id) {
            let order = match side {
                LegSide::Up => quotes.up_order.take(),
                LegSide::Down => quotes.down_order.take(),
            };
            if let Some(o) = order {
                let _ = self.client.cancel_order(&o.order_id).await;
            }
        }

        self.post_taker_order(&token_id, price, size, market_id).await;
    }

    async fn maintain_all_quotes(&mut self) {
        let market_ids: Vec<String> = self.markets.keys().cloned().collect();

        for market_id in market_ids {
            self.maintain_market_quotes(&market_id).await;
        }
    }

    async fn maintain_market_quotes(&mut self, market_id: &str) {
        // Extract all needed data upfront to avoid borrow conflicts
        let (up_token_id, down_token_id, target_combined, shares_per_trade, target_shares) = {
            let state = match self.markets.get(market_id) {
                Some(s) => s,
                None => return,
            };
            let cfg = &self.config.legging_config;
            (
                state.info.up_token_id.clone(),
                state.info.down_token_id.clone(),
                cfg.target_combined,
                cfg.shares_per_trade,
                cfg.target_shares_per_market,
            )
        };

        let current_exposure = {
            let inv = self.ws_inventory.read();
            inv.get(market_id)
                .map(|i| i.up_shares.max(i.down_shares))
                .unwrap_or(dec!(0))
        };

        if current_exposure >= target_shares {
            self.cancel_market_quotes(market_id).await;
            return;
        }

        let (up_ask, down_ask) = {
            let cache = self.ws_price_cache.read();
            let up = cache.get(&up_token_id).map(|(_, a, _)| *a);
            let down = cache.get(&down_token_id).map(|(_, a, _)| *a);
            match (up, down) {
                (Some(u), Some(d)) => (u, d),
                _ => return,
            }
        };

        // If down_ask = 0.82 and target_combined = 0.98, then up_bid = 0.16
        let up_bid_target = target_combined - down_ask;
        let down_bid_target = target_combined - up_ask;
        let size = shares_per_trade.min(target_shares - current_exposure);

        // Don't post if remaining capacity is below minimum
        if size < MIN_ORDER_SIZE {
            debug!(
                "Remaining capacity {} < {} for {}, skipping new quotes",
                size, MIN_ORDER_SIZE, market_id
            );
            return;
        }

        self.update_quote(market_id, LegSide::Up, &up_token_id, up_bid_target, size).await;
        self.update_quote(market_id, LegSide::Down, &down_token_id, down_bid_target, size).await;
    }

    async fn update_quote(
        &mut self,
        market_id: &str,
        side: LegSide,
        token_id: &str,
        target_price: Decimal,
        size: Decimal,
    ) {
        let target_price = clamp_price(target_price);

        // Don't post bids too close to edges
        if target_price < MIN_PRICE + PRICE_BUFFER || target_price > MAX_PRICE - PRICE_BUFFER {
            self.cancel_quote(market_id, side).await;
            return;
        }

        let quotes = match self.market_quotes.get_mut(market_id) {
            Some(q) => q,
            None => return,
        };

        let current_order = match side {
            LegSide::Up => &quotes.up_order,
            LegSide::Down => &quotes.down_order,
        };

        let price_threshold = self.config.legging_config.requote_threshold;

        if let Some(order) = current_order {
            let price_diff = (order.price - target_price).abs();

            if price_diff <= price_threshold {
                // Price hasn't moved enough, keep existing order
                return;
            }

            // Don't requote too frequently - let orders have time to get filled
            let order_age = order.posted_at.elapsed();
            if order_age < MIN_REQUOTE_INTERVAL {
                // Order is too fresh, don't requote yet
                return;
            }

            // Cancel existing order
            debug!(
                "Requoting {:?}: {} -> {} (diff {}, age {:?})",
                side, order.price, target_price, price_diff, order_age
            );
            let _ = self.client.cancel_order(&order.order_id).await;
        }

        // Post new order
        match self.post_maker_order(token_id, target_price, size, market_id).await {
            Some(order_id) => {
                let order = RestingOrder {
                    order_id,
                    token_id: token_id.to_string(),
                    price: target_price,
                    size,
                    filled: Decimal::ZERO,
                    posted_at: Instant::now(),
                };

                let quotes = self.market_quotes.get_mut(market_id).unwrap();
                match side {
                    LegSide::Up => quotes.up_order = Some(order),
                    LegSide::Down => quotes.down_order = Some(order),
                }

                info!(
                    "Posted {:?} bid @ {} for {}",
                    side, target_price, market_id 
                );
            }
            None => {
                warn!("Failed to post {:?} bid", side);
            }
        }
    }

    async fn cancel_quote(&mut self, market_id: &str, side: LegSide) {
        let quotes = match self.market_quotes.get_mut(market_id) {
            Some(q) => q,
            None => return,
        };

        let order = match side {
            LegSide::Up => quotes.up_order.take(),
            LegSide::Down => quotes.down_order.take(),
        };

        if let Some(o) = order {
            let _ = self.client.cancel_order(&o.order_id).await;
            debug!("Cancelled {:?} order {}", side, o.order_id);
        }
    }

    async fn cancel_market_quotes(&mut self, market_id: &str) {
        self.cancel_quote(market_id, LegSide::Up).await;
        self.cancel_quote(market_id, LegSide::Down).await;
    }

    async fn cancel_all_orders(&mut self) {
        info!("Cancelling all orders");
        let _ = self.client.cancel_all_orders().await;
        for quotes in self.market_quotes.values_mut() {
            quotes.up_order = None;
            quotes.down_order = None;
        }
    }

    async fn post_maker_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        market_id: &str,
    ) -> Option<String> {
        if self.config.dry_run {
            info!(
                "[DRY] maker bid {} @ {} ({})",
                size, price, market_id
            );
            return Some(format!("dry-{}", Utc::now().timestamp_millis()));
        }

        let size_poly = PolyDecimal::try_from(size.to_string().as_str()).ok()?;
        let price_poly = PolyDecimal::try_from(price.to_string().as_str()).ok()?;

        match self
            .client
            .limit_order()
            .token_id(token_id)
            .price(price_poly)
            .size(size_poly)
            .side(ClobSide::Buy)
            .order_type(OrderType::GTC) // Good Till Cancel - resting order
            .build()
            .await
        {
            Ok(signable) => match self.client.sign(&self.signer_with_chain, signable).await {
                Ok(signed) => match self.client.post_order(signed).await {
                    Ok(resp) => Some(resp.order_id),
                    Err(e) => {
                        warn!("Failed to post maker order: {}", e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Failed to sign maker order: {}", e);
                    None
                }
            },
            Err(e) => {
                warn!("Failed to build maker order: {}", e);
                None
            }
        }
    }

    async fn post_taker_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        market_id: &str,
    ) -> Option<String> {
        if self.config.dry_run {
            info!(
                "[DRY] taker hit {} @ {} ({})",
                size, price, market_id
            );
            return Some(format!("dry-{}", Utc::now().timestamp_millis()));
        }

        let size_poly = PolyDecimal::try_from(size.to_string().as_str()).ok()?;
        let price_poly = PolyDecimal::try_from(price.to_string().as_str()).ok()?;

        match self
            .client
            .limit_order()
            .token_id(token_id)
            .price(price_poly)
            .size(size_poly)
            .side(ClobSide::Buy)
            .order_type(OrderType::FOK) // Fill Or Kill - aggressive
            .build()
            .await
        {
            Ok(signable) => match self.client.sign(&self.signer_with_chain, signable).await {
                Ok(signed) => match self.client.post_order(signed).await {
                    Ok(resp) => {
                        info!(
                            "Taker order filled: {} @ {} ({})",
                            size, price, resp.order_id
                        );
                        Some(resp.order_id)
                    }
                    Err(e) => {
                        warn!("Taker order failed: {}", e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Failed to sign taker order: {}", e);
                    None
                }
            },
            Err(e) => {
                warn!("Failed to build taker order: {}", e);
                None
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