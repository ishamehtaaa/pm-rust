use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{MarketIds, MarketInfo, MarketInventory, MarketLookup, MarketQuotes, MarketState, OrderEvent, RestingOrder, SecondLegParams, Side};

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use futures_util::{future::join_all, stream::StreamExt};
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::OrderType;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::ws::{self, BookUpdate, WsMessage};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const STATUS_PRINT_INTERVAL: Duration = Duration::from_secs(10);
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const LIMIT_TICK_DP: u32 = 2;
const PRICE_BUFFER: Decimal = dec!(0.01);
const MIN_ORDER_SIZE: Decimal = dec!(5.0);
const IMBALANCE_THRESHOLD: Decimal = dec!(2.0);
const LEVELS_PER_SIDE: usize = 3;
const PRICE_TICK: Decimal = dec!(0.01);
const MIN_NOTIONAL_USD: Decimal = dec!(1.00);

fn round_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
}

fn round_size_for_taker(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero)
}

fn round_to_tick(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(LIMIT_TICK_DP, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
}

fn clamp_price(price: Decimal) -> Decimal {
    round_to_tick(price).max(MIN_PRICE).min(MAX_PRICE)
}

fn ensure_min_notional_whole_shares(size: Decimal, price: Decimal) -> Decimal {
    if price <= Decimal::ZERO {
        return size;
    }
    let required = (MIN_NOTIONAL_USD / price)
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::AwayFromZero);
    size.max(required)
}

/// Per-market state for the reactive model
#[derive(Debug)]
pub struct MarketExecutionState {
    /// True if a taker is currently in flight for this market
    pub taker_in_flight: bool,
    /// Last time we attempted a taker (for cooldown after failures)
    pub last_taker_attempt: Option<Instant>,
    /// True if last taker failed with balance error
    pub balance_error_cooldown: bool,
    /// Confirmed inventory (from WS fill events only)
    pub inventory: MarketInventory,
    /// Our resting orders
    pub resting_orders: MarketQuotes,
    /// Last known prices
    pub up_bid: Option<Decimal>,
    pub up_ask: Option<Decimal>,
    pub down_bid: Option<Decimal>,
    pub down_ask: Option<Decimal>,
}

impl Default for MarketExecutionState {
    fn default() -> Self {
        Self {
            taker_in_flight: false,
            last_taker_attempt: None,
            balance_error_cooldown: false,
            inventory: MarketInventory::default(),
            resting_orders: MarketQuotes::default(),
            up_bid: None,
            up_ask: None,
            down_bid: None,
            down_ask: None,
        }
    }
}

pub struct LeggingBot {
    pub config: Config,
    pub client: Arc<AuthenticatedClient>,
    pub signer_with_chain: PrivateKeySigner,
    pub trader_address: Address,
    pub signature_type: SignatureType,
    
    pub ws_market: ws::Client,
    pub ws_user: ws::Client<Authenticated<Normal>>,

    pub market_cache: MarketCache,
    pub markets: HashMap<String, MarketState>,
    
    /// Per-market execution state - this is the source of truth
    pub execution_state: HashMap<String, MarketExecutionState>,
    
    /// Channel for order events from WS
    pub order_tx: mpsc::Sender<OrderEvent>,
    pub order_rx: mpsc::Receiver<OrderEvent>,
    
    /// Channel for price updates from WS  
    pub price_tx: mpsc::Sender<PriceUpdate>,
    pub price_rx: mpsc::Receiver<PriceUpdate>,
    
    /// Channel for taker results
    pub taker_result_tx: mpsc::Sender<TakerResult>,
    pub taker_result_rx: mpsc::Receiver<TakerResult>,
    
    pub ws_tasks: Vec<tokio::task::JoinHandle<()>>,
    pub current_window_end: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub token_id: String,
    pub bid: Decimal,
    pub ask: Decimal,
}

#[derive(Debug, Clone)]
pub struct TakerResult {
    pub gamma_id: String,
    pub success: bool,
    pub is_balance_error: bool,
    pub side: Side,
    pub size: Decimal,
    pub price: Decimal,
}

impl LeggingBot {
    #[instrument(skip(config))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing Reactive Legging Bot");

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

        let (order_tx, order_rx) = mpsc::channel::<OrderEvent>(256);
        let (price_tx, price_rx) = mpsc::channel::<PriceUpdate>(1024);
        let (taker_result_tx, taker_result_rx) = mpsc::channel::<TakerResult>(64);

        Ok(Self {
            config,
            client: Arc::new(client),
            signer_with_chain,
            trader_address,
            signature_type,
            ws_market,
            ws_user,
            market_cache,
            markets: HashMap::new(),
            execution_state: HashMap::new(),
            order_tx,
            order_rx,
            price_tx,
            price_rx,
            taker_result_tx,
            taker_result_rx,
            ws_tasks: Vec::new(),
            current_window_end: None,
        })
    }

    fn get_asset_name(&self, gamma_id: &str) -> String {
        self.markets
            .get(gamma_id)
            .map(|s| s.info.asset.to_uppercase())
            .unwrap_or_else(|| gamma_id.to_string())
    }

    pub async fn run(&mut self) {
        let mut status_tick = tokio::time::interval(STATUS_PRINT_INTERVAL);
        let mut rollover_tick = tokio::time::interval(Duration::from_secs(1));
        let mut ladder_tick = tokio::time::interval(Duration::from_secs(5)); // Ladder maintenance less frequent

        loop {
            tokio::select! {
                // HIGHEST PRIORITY: Order fill events
                Some(event) = self.order_rx.recv() => {
                    self.handle_order_event(event).await;
                }
                
                // HIGH PRIORITY: Taker results
                Some(result) = self.taker_result_rx.recv() => {
                    self.handle_taker_result(result).await;
                }
                
                // MEDIUM PRIORITY: Price updates - may trigger rebalance
                Some(update) = self.price_rx.recv() => {
                    self.handle_price_update(update).await;
                }
                
                // LOW PRIORITY: Periodic ladder maintenance
                _ = ladder_tick.tick() => {
                    self.maintain_ladders().await;
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

    /// Handle a fill/placement/cancellation event from WS
    async fn handle_order_event(&mut self, event: OrderEvent) {
        let gamma_id = event.gamma_id.clone();
        let order_id = event.order_id.clone();
        let asset = self.get_asset_name(&gamma_id);
        
        let (should_check_second_leg, second_leg_params) = {
            let state = match self.execution_state.get_mut(&gamma_id) {
                Some(s) => s,
                None => return,
            };

            match event.msg_type.as_str() {
                "PLACEMENT" => {
                    debug!("Order {} confirmed", order_id);
                    (false, None)
                }
                "CANCELLATION" => {
                    state.resting_orders.clear_order_by_id(&order_id);
                    (false, None)
                }
                _ => {
                    // Fill event
                    let (prev_filled, second_leg_params) = state.resting_orders
                        .find_order_by_id(&order_id)
                        .map(|(o, _)| (o.filled, o.second_leg.clone()))
                        .unwrap_or((Decimal::ZERO, None));

                    if event.size_matched > prev_filled {
                        let fill_amount = event.size_matched - prev_filled;
                        
                        // Update confirmed inventory
                        state.inventory.add_buy(event.side, fill_amount, event.price);
                        
                        // Update resting order tracking
                        if let Some((o, _)) = state.resting_orders.find_order_mut_by_id(&order_id) {
                            o.filled = event.size_matched;
                            if o.is_fully_filled() {
                                state.resting_orders.clear_order_by_id(&order_id);
                            }
                        }

                        info!("[{}] FILL: {} {:?} @ {} | new inv: {} up / {} down",
                            asset, fill_amount, event.side, event.price,
                            state.inventory.up_shares, state.inventory.down_shares);

                        // Check if we need second leg - only if no taker in flight
                        (!state.taker_in_flight, second_leg_params)
                    } else {
                        (false, None)
                    }
                }
            }
        };
        
        if should_check_second_leg {
            self.maybe_execute_second_leg(&gamma_id, second_leg_params).await;
        }
    }

    /// Handle taker completion
    async fn handle_taker_result(&mut self, result: TakerResult) {
        let state = match self.execution_state.get_mut(&result.gamma_id) {
            Some(s) => s,
            None => return,
        };

        state.taker_in_flight = false;

        if result.success {
            state.balance_error_cooldown = false;
            // Inventory will be updated via WS fill event
            let asset = self.get_asset_name(&result.gamma_id);
            info!("[{}] Taker success: {} {:?} @ {}", asset, result.size, result.side, result.price);
        } else {
            if result.is_balance_error {
                state.balance_error_cooldown = true;
                state.last_taker_attempt = Some(Instant::now());
                let asset = self.get_asset_name(&result.gamma_id);
                warn!("[{}] Balance error - cooldown 30s", asset);
            }
        }
    }

    /// Handle price update - check for rebalance opportunity
    async fn handle_price_update(&mut self, update: PriceUpdate) {
        // Find which market this token belongs to
        let gamma_id = self.find_gamma_for_token(&update.token_id);
        let gamma_id = match gamma_id {
            Some(id) => id,
            None => return,
        };

        let market_info = match self.markets.get(&gamma_id) {
            Some(m) => m.info.clone(),
            None => return,
        };

        // Update price cache
        let state = match self.execution_state.get_mut(&gamma_id) {
            Some(s) => s,
            None => return,
        };

        if update.token_id == market_info.ids.up_token {
            state.up_bid = Some(update.bid);
            state.up_ask = Some(update.ask);
        } else {
            state.down_bid = Some(update.bid);
            state.down_ask = Some(update.ask);
        }

        // Check for rebalance opportunity if no taker in flight
        if !state.taker_in_flight {
            self.check_rebalance(&gamma_id).await;
        }
    }

    fn find_gamma_for_token(&self, token_id: &str) -> Option<String> {
        for (gamma_id, market) in &self.markets {
            if market.info.ids.up_token == token_id || market.info.ids.down_token == token_id {
                return Some(gamma_id.clone());
            }
        }
        None
    }

    /// Check if we should rebalance (buy deficit side at market)
    async fn check_rebalance(&mut self, gamma_id: &str) {
        let target_combined = self.config.legging_config.target_combined;
        let target_shares = self.config.legging_config.target_shares_per_market;

        let state = match self.execution_state.get(gamma_id) {
            Some(s) => s,
            None => return,
        };

        // Skip if taker in flight or in cooldown
        if state.taker_in_flight {
            return;
        }
        if state.balance_error_cooldown {
            if let Some(t) = state.last_taker_attempt {
                if t.elapsed() < Duration::from_secs(30) {
                    return;
                }
            }
        }

        let imbalance = state.inventory.imbalance();
        if imbalance.abs() < dec!(1.0) {
            return; // Balanced enough
        }

        let (needed_side, excess_avg, current_ask) = if imbalance > dec!(0) {
            // More Up than Down - need Down
            let avg = state.inventory.avg_price(Side::Up).unwrap_or(dec!(0.50));
            let ask = match state.down_ask {
                Some(a) => a,
                None => return,
            };
            (Side::Down, avg, ask)
        } else {
            // More Down than Up - need Up
            let avg = state.inventory.avg_price(Side::Down).unwrap_or(dec!(0.50));
            let ask = match state.up_ask {
                Some(a) => a,
                None => return,
            };
            (Side::Up, avg, ask)
        };

        // Check profitability
        let combined_cost = excess_avg + current_ask;
        if combined_cost > target_combined {
            return; // Not profitable
        }
        
        let profit_per_share = dec!(1.0) - combined_cost;
        if profit_per_share < dec!(0.005) {
            return; // Margin too thin
        }

        // Check capacity
        let side_shares = match needed_side {
            Side::Up => state.inventory.up_shares,
            Side::Down => state.inventory.down_shares,
        };
        let side_resting: Decimal = match needed_side {
            Side::Up => state.resting_orders.up_orders.iter().map(|o| o.remaining()).sum(),
            Side::Down => state.resting_orders.down_orders.iter().map(|o| o.remaining()).sum(),
        };
        let available = (target_shares - side_shares - side_resting).max(dec!(0));
        
        if available < MIN_ORDER_SIZE {
            return; // At capacity
        }

        // Calculate size
        let size = round_size_for_taker(imbalance.abs().min(available));
        if size < MIN_ORDER_SIZE {
            return;
        }

        let taker_price = clamp_price(current_ask + dec!(0.01));
        
        // Execute the taker
        self.execute_taker(gamma_id, needed_side, size, taker_price).await;
    }

    /// Execute second leg after a maker fill
    async fn maybe_execute_second_leg(&mut self, gamma_id: &str, precomputed: Option<SecondLegParams>) {
        let target_combined = self.config.legging_config.target_combined;
        let target_shares = self.config.legging_config.target_shares_per_market;

        let state = match self.execution_state.get(gamma_id) {
            Some(s) => s,
            None => return,
        };

        if state.taker_in_flight {
            return;
        }

        let imbalance = state.inventory.imbalance();
        if imbalance.abs() < dec!(1.0) {
            return;
        }

        let needed_side = if imbalance > dec!(0) { Side::Down } else { Side::Up };

        // Get first leg average
        let first_leg_side = if imbalance > dec!(0) { Side::Up } else { Side::Down };
        let avg_first_leg = state.inventory.avg_price(first_leg_side).unwrap_or(dec!(0.50));
        let max_second_price = target_combined - avg_first_leg;

        // Get current ask for needed side
        let current_ask = match needed_side {
            Side::Up => state.up_ask,
            Side::Down => state.down_ask,
        };
        let current_ask = match current_ask {
            Some(a) => a,
            None => return,
        };

        // Use precomputed max_price if available and matches side
        let max_price = match precomputed {
            Some(ref p) if p.side == needed_side => p.max_price,
            _ => max_second_price,
        };

        if current_ask > max_price {
            return; // Not profitable
        }

        // Check capacity
        let side_shares = match needed_side {
            Side::Up => state.inventory.up_shares,
            Side::Down => state.inventory.down_shares,
        };
        let side_resting: Decimal = match needed_side {
            Side::Up => state.resting_orders.up_orders.iter().map(|o| o.remaining()).sum(),
            Side::Down => state.resting_orders.down_orders.iter().map(|o| o.remaining()).sum(),
        };
        let available = (target_shares - side_shares - side_resting).max(dec!(0));

        if available < MIN_ORDER_SIZE {
            return;
        }

        let size = round_size_for_taker(imbalance.abs().min(available));
        if size < MIN_ORDER_SIZE {
            return;
        }

        let taker_price = clamp_price(current_ask + dec!(0.01));
        
        // Profitability check
        let combined_cost = avg_first_leg + taker_price;
        let profit = dec!(1.0) - combined_cost;
        if profit < dec!(0.005) {
            return;
        }

        self.execute_taker(gamma_id, needed_side, size, taker_price).await;
    }

    /// Actually execute a taker order
    async fn execute_taker(&mut self, gamma_id: &str, side: Side, size: Decimal, price: Decimal) {
        // Get market info and asset name first (immutable borrows)
        let market_info = match self.markets.get(gamma_id) {
            Some(m) => m.info.clone(),
            None => return,
        };
        let asset = self.get_asset_name(gamma_id);
        let token_id = market_info.ids.token_for_side(side).to_string();

        // Now do mutable borrow
        let orders_to_cancel = {
            let state = match self.execution_state.get_mut(gamma_id) {
                Some(s) => s,
                None => return,
            };

            // Double check no taker in flight
            if state.taker_in_flight {
                return;
            }

            // Mark taker as in flight BEFORE spawning
            state.taker_in_flight = true;
            state.last_taker_attempt = Some(Instant::now());

            // Collect orders to cancel
            state.resting_orders
                .side_orders_mut(side)
                .drain(..)
                .map(|o| o.order_id)
                .collect::<Vec<String>>()
        };

        info!("[{}] TAKER: {} {:?} @ {}", asset, size, side, price);

        let client = self.client.clone();
        let signer = self.signer_with_chain.clone();
        let result_tx = self.taker_result_tx.clone();
        let gamma_id_owned = gamma_id.to_string();
        let dry_run = self.config.dry_run;

        tokio::spawn(async move {
            // Cancel orders first
            if !orders_to_cancel.is_empty() {
                let cancel_futures: Vec<_> = orders_to_cancel.into_iter().map(|oid| {
                    let c = client.clone();
                    async move { c.cancel_order(&oid).await }
                }).collect();
                join_all(cancel_futures).await;
            }

            if dry_run {
                info!("[{}] [DRY] taker {} {:?} @ {}", asset, size, side, price);
                let _ = result_tx.send(TakerResult {
                    gamma_id: gamma_id_owned,
                    success: true,
                    is_balance_error: false,
                    side,
                    size,
                    price,
                }).await;
                return;
            }

            let size_poly = match PolyDecimal::try_from(size.to_string().as_str()) {
                Ok(v) => v,
                Err(_) => {
                    let _ = result_tx.send(TakerResult {
                        gamma_id: gamma_id_owned, success: false, is_balance_error: false, side, size, price,
                    }).await;
                    return;
                }
            };
            let price_poly = match PolyDecimal::try_from(price.to_string().as_str()) {
                Ok(v) => v,
                Err(_) => {
                    let _ = result_tx.send(TakerResult {
                        gamma_id: gamma_id_owned, success: false, is_balance_error: false, side, size, price,
                    }).await;
                    return;
                }
            };

            let signable = match client.limit_order()
                .token_id(&token_id)
                .price(price_poly)
                .size(size_poly)
                .side(ClobSide::Buy)
                .order_type(OrderType::FOK)
                .build()
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to build taker: {}", asset, e);
                    let _ = result_tx.send(TakerResult {
                        gamma_id: gamma_id_owned, success: false, is_balance_error: false, side, size, price,
                    }).await;
                    return;
                }
            };

            let signed = match client.sign(&signer, signable).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("[{}] Failed to sign: {}", asset, e);
                    let _ = result_tx.send(TakerResult {
                        gamma_id: gamma_id_owned, success: false, is_balance_error: false, side, size, price,
                    }).await;
                    return;
                }
            };

            match client.post_order(signed).await {
                Ok(resp) => {
                    info!("[{}] TAKER FILLED: {} {:?} @ {} ({})", asset, size, side, price, resp.order_id);
                    let _ = result_tx.send(TakerResult {
                        gamma_id: gamma_id_owned, success: true, is_balance_error: false, side, size, price,
                    }).await;
                }
                Err(e) => {
                    let err_str = e.to_string();
                    let is_balance = err_str.contains("balance") || err_str.contains("allowance");
                    warn!("[{}] TAKER REJECTED: {}", asset, e);
                    let _ = result_tx.send(TakerResult {
                        gamma_id: gamma_id_owned, success: false, is_balance_error: is_balance, side, size, price,
                    }).await;
                }
            }
        });
    }

    /// Maintain maker ladders - called less frequently
    async fn maintain_ladders(&mut self) {
        let gamma_ids: Vec<String> = self.markets.keys().cloned().collect();
        for gamma_id in gamma_ids {
            self.maintain_market_ladder(&gamma_id).await;
        }
    }

    async fn maintain_market_ladder(&mut self, gamma_id: &str) {
        let target_combined = self.config.legging_config.target_combined;
        let shares_per_trade = self.config.legging_config.shares_per_trade;
        let target_shares = self.config.legging_config.target_shares_per_market;

        let market_info = match self.markets.get(gamma_id) {
            Some(m) => m.info.clone(),
            None => return,
        };

        let state = match self.execution_state.get_mut(gamma_id) {
            Some(s) => s,
            None => return,
        };

        let (up_ask, down_ask) = match (state.up_ask, state.down_ask) {
            (Some(u), Some(d)) => (u, d),
            _ => return,
        };

        let imbalance = state.inventory.imbalance();
        
        // Calculate bid targets based on inventory
        let up_bid_target = match state.inventory.avg_price(Side::Down) {
            Some(avg) if state.inventory.down_shares > dec!(0) => target_combined - avg,
            _ => target_combined - down_ask,
        };
        let down_bid_target = match state.inventory.avg_price(Side::Up) {
            Some(avg) if state.inventory.up_shares > dec!(0) => target_combined - avg,
            _ => target_combined - up_ask,
        };

        // Calculate capacity
        let up_resting: Decimal = state.resting_orders.up_orders.iter().map(|o| o.remaining()).sum();
        let down_resting: Decimal = state.resting_orders.down_orders.iter().map(|o| o.remaining()).sum();
        let up_capacity = (target_shares - state.inventory.up_shares - up_resting).max(dec!(0));
        let down_capacity = (target_shares - state.inventory.down_shares - down_resting).max(dec!(0));

        if imbalance > IMBALANCE_THRESHOLD {
            // Only maintain down ladder
            self.cancel_side_orders(gamma_id, Side::Up).await;
            if down_capacity >= MIN_ORDER_SIZE {
                self.maintain_side_ladder(gamma_id, Side::Down, &market_info.ids, down_bid_target, shares_per_trade, down_capacity, target_combined).await;
            }
        } else if imbalance < -IMBALANCE_THRESHOLD {
            // Only maintain up ladder
            self.cancel_side_orders(gamma_id, Side::Down).await;
            if up_capacity >= MIN_ORDER_SIZE {
                self.maintain_side_ladder(gamma_id, Side::Up, &market_info.ids, up_bid_target, shares_per_trade, up_capacity, target_combined).await;
            }
        } else {
            // Maintain both
            if up_capacity >= MIN_ORDER_SIZE {
                self.maintain_side_ladder(gamma_id, Side::Up, &market_info.ids, up_bid_target, shares_per_trade, up_capacity, target_combined).await;
            } else {
                self.cancel_side_orders(gamma_id, Side::Up).await;
            }
            if down_capacity >= MIN_ORDER_SIZE {
                self.maintain_side_ladder(gamma_id, Side::Down, &market_info.ids, down_bid_target, shares_per_trade, down_capacity, target_combined).await;
            } else {
                self.cancel_side_orders(gamma_id, Side::Down).await;
            }
        }
    }

    async fn maintain_side_ladder(
        &mut self,
        gamma_id: &str,
        side: Side,
        ids: &MarketIds,
        base_price: Decimal,
        shares_per_trade: Decimal,
        capacity: Decimal,
        target_combined: Decimal,
    ) {
        let requote_threshold = self.config.legging_config.requote_threshold;
        let base_price = clamp_price(base_price);

        if base_price < MIN_PRICE + PRICE_BUFFER || base_price > MAX_PRICE - PRICE_BUFFER {
            self.cancel_side_orders(gamma_id, side).await;
            return;
        }

        // Build desired levels
        let mut levels: Vec<Decimal> = Vec::new();
        let mut cap_left = capacity;
        for i in 0..LEVELS_PER_SIDE {
            let price = clamp_price(base_price - PRICE_TICK * Decimal::from(i as i64));
            if price < MIN_PRICE + PRICE_BUFFER || cap_left < MIN_ORDER_SIZE {
                break;
            }
            levels.push(price);
            cap_left -= shares_per_trade.min(cap_left);
        }

        if levels.is_empty() {
            self.cancel_side_orders(gamma_id, side).await;
            return;
        }

        let state = match self.execution_state.get_mut(gamma_id) {
            Some(s) => s,
            None => return,
        };

        let ladder_top = levels[0];
        let ladder_bottom = levels[levels.len() - 1];

        // Partition existing orders
        let existing = std::mem::take(state.resting_orders.side_orders_mut(side));
        let mut keep = Vec::new();
        let mut stale_ids = Vec::new();

        for order in existing {
            let too_high = order.price > ladder_top + requote_threshold;
            let too_low = order.price < ladder_bottom - requote_threshold;
            if too_high || too_low {
                stale_ids.push(order.order_id.clone());
            } else {
                keep.push(order);
            }
        }

        // Cancel stale
        if !stale_ids.is_empty() {
            let client = self.client.clone();
            let futures: Vec<_> = stale_ids.into_iter().map(|oid| {
                let c = client.clone();
                async move { c.cancel_order(&oid).await }
            }).collect();
            join_all(futures).await;
        }

        // Restore kept
        *state.resting_orders.side_orders_mut(side) = keep.clone();

        // Find uncovered levels
        let covered: std::collections::HashSet<Decimal> = keep.iter().map(|o| round_to_tick(o.price)).collect();
        let uncovered: Vec<Decimal> = levels.into_iter()
            .filter(|p| !covered.iter().any(|c| (*c - *p).abs() <= requote_threshold))
            .collect();

        if uncovered.is_empty() {
            return;
        }

        // Post new orders
        let second_side = match side { Side::Up => Side::Down, Side::Down => Side::Up };
        let second_token = ids.token_for_side(second_side).to_string();
        let token_id = ids.token_for_side(side).to_string();

        for price in uncovered {
            let size = ensure_min_notional_whole_shares(shares_per_trade, price);
            let second_leg = SecondLegParams {
                token_id: second_token.clone(),
                side: second_side,
                max_price: target_combined - price,
            };

            let order_id = self.post_maker_order(&token_id, price, size, gamma_id).await;
            if let Some(oid) = order_id {
                let order = RestingOrder {
                    order_id: oid,
                    token_id: token_id.clone(),
                    price,
                    size,
                    filled: Decimal::ZERO,
                    posted_at: Instant::now(),
                    second_leg: Some(second_leg),
                };
                if let Some(s) = self.execution_state.get_mut(gamma_id) {
                    s.resting_orders.side_orders_mut(side).push(order);
                }
            }
        }
    }

    async fn cancel_side_orders(&mut self, gamma_id: &str, side: Side) {
        let state = match self.execution_state.get_mut(gamma_id) {
            Some(s) => s,
            None => return,
        };

        let to_cancel: Vec<String> = state.resting_orders
            .side_orders_mut(side)
            .drain(..)
            .map(|o| o.order_id)
            .collect();

        if to_cancel.is_empty() {
            return;
        }

        let client = self.client.clone();
        let futures: Vec<_> = to_cancel.into_iter().map(|oid| {
            let c = client.clone();
            async move { c.cancel_order(&oid).await }
        }).collect();
        join_all(futures).await;
    }

    async fn cancel_all_orders(&mut self) {
        info!("Cancelling all orders");
        let _ = self.client.cancel_all_orders().await;
        for state in self.execution_state.values_mut() {
            state.resting_orders.up_orders.clear();
            state.resting_orders.down_orders.clear();
        }
    }

    async fn post_maker_order(&self, token_id: &str, price: Decimal, size: Decimal, gamma_id: &str) -> Option<String> {
        if self.config.dry_run {
            let asset = self.get_asset_name(gamma_id);
            info!("[{}] [DRY] maker {} @ {}", asset, size, price);
            return Some(format!("dry-{}", Utc::now().timestamp_millis()));
        }

        let size_poly = PolyDecimal::try_from(round_size(size).to_string().as_str()).ok()?;
        let price_poly = PolyDecimal::try_from(price.to_string().as_str()).ok()?;

        let signable = self.client.limit_order()
            .token_id(token_id)
            .price(price_poly)
            .size(size_poly)
            .side(ClobSide::Buy)
            .order_type(OrderType::GTC)
            .build()
            .await.ok()?;

        let signed = self.client.sign(&self.signer_with_chain, signable).await.ok()?;
        self.client.post_order(signed).await.ok().map(|r| r.order_id)
    }

    async fn check_market_rollover(&mut self) {
        let now = Utc::now();
        let window_expired = match self.current_window_end {
            Some(end) => now >= end,
            None => true,
        };

        if window_expired {
            info!("Market window expired, refreshing");
            self.cancel_all_orders().await;
            self.discover_markets().await;
        }
    }

    fn log_status(&self) {
        info!("=== STATUS: {} markets ===", self.markets.len());
        for (gamma_id, market) in &self.markets {
            let asset = market.info.asset.to_uppercase();
            let state = match self.execution_state.get(gamma_id) {
                Some(s) => s,
                None => continue,
            };

            let inv = &state.inventory;
            let up_avg = inv.avg_price(Side::Up);
            let down_avg = inv.avg_price(Side::Down);
            let imbalance = inv.imbalance();
            let pairs = inv.up_shares.min(inv.down_shares);
            let pnl = if pairs > dec!(0) {
                let up_cost = up_avg.unwrap_or(dec!(0)) * pairs;
                let down_cost = down_avg.unwrap_or(dec!(0)) * pairs;
                pairs - (up_cost + down_cost)
            } else {
                dec!(0)
            };

            info!("[{}] inv: {} up (avg {:?}) / {} down (avg {:?}) | imb: {} | pairs: {} | pnl: ${:.3}",
                asset, inv.up_shares, up_avg, inv.down_shares, down_avg, imbalance, pairs, pnl);
            info!("[{}] book: up {:?}/{:?} | down {:?}/{:?} | taker_in_flight: {}",
                asset, state.up_bid, state.up_ask, state.down_bid, state.down_ask, state.taker_in_flight);
        }
    }

    pub async fn discover_markets(&mut self) {
        let now = Utc::now();
        let found = match self.market_cache.get_markets(now).await {
            Ok(markets) => markets,
            Err(e) => {
                warn!("Failed to fetch markets: {}", e);
                return;
            }
        };

        self.markets.clear();
        self.execution_state.clear();

        let mut earliest_end: Option<DateTime<Utc>> = None;
        let mut token_ids = Vec::new();
        let mut gamma_ids = Vec::new();

        for market in found {
            let gamma_id = market.ids.condition_id.clone();
            
            self.markets.insert(gamma_id.clone(), MarketState {
                info: market.clone(),
                first_leg_filled: false,
            });
            self.execution_state.insert(gamma_id.clone(), MarketExecutionState::default());

            token_ids.push(market.ids.up_token.clone());
            token_ids.push(market.ids.down_token.clone());
            gamma_ids.push(gamma_id);

            match earliest_end {
                None => earliest_end = Some(market.end_time),
                Some(e) if market.end_time < e => earliest_end = Some(market.end_time),
                _ => {}
            }
        }

        self.current_window_end = earliest_end;
        info!("Discovered {} markets, window ends at {:?}", self.markets.len(), earliest_end);

        if token_ids.is_empty() {
            return;
        }

        // Start WS subscriptions
        self.start_ws_subscriptions(token_ids, gamma_ids).await;
    }

    async fn start_ws_subscriptions(&mut self, token_ids: Vec<String>, gamma_ids: Vec<String>) {
        // Build lookup for resolving tokens to markets
        let lookup = MarketLookup::new(&self.markets);
        
        // Orderbook subscription with reconnect loop
        let orderbook_client = self.ws_market.clone();
        let orderbook_ids = token_ids.clone();
        let price_tx = self.price_tx.clone();

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
                                Ok(BookUpdate { asset_id, bids, asks, .. }) => {
                                    let best_bid = bids.iter().max_by_key(|l| l.price).map(|l| l.price);
                                    let best_ask = asks.iter().min_by_key(|l| l.price).map(|l| l.price);

                                    if let (Some(b), Some(a)) = (best_bid, best_ask) {
                                        if let (Ok(bid), Ok(ask)) = (
                                            b.to_string().parse::<Decimal>(),
                                            a.to_string().parse::<Decimal>(),
                                        ) {
                                            let _ = price_tx.send(PriceUpdate {
                                                token_id: asset_id,
                                                bid,
                                                ask,
                                            }).await;
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

        // User events subscription with reconnect loop
        let user_client = self.ws_user.clone();
        let user_condition_ids = gamma_ids.clone();
        let user_lookup = lookup.clone();
        let order_tx = self.order_tx.clone();

        self.ws_tasks.push(tokio::spawn(async move {
            let mut backoff = Duration::from_millis(500);

            loop {
                match user_client.subscribe_user_events(user_condition_ids.clone()) {
                    Ok(sub) => {
                        info!("Subscribed to user events for {} markets", user_condition_ids.len());
                        backoff = Duration::from_millis(500);
                        let mut stream = Box::pin(sub);

                        while let Some(msg) = stream.next().await {
                            match msg {
                                Ok(WsMessage::Order(o)) => {
                                    let msg_type = o.msg_type.clone().unwrap_or_default();
                                    let size_matched = o.size_matched.unwrap_or_default();

                                    let gamma_id = match user_lookup.resolve_condition(&o.market) {
                                        Some(id) => id.to_string(),
                                        None => continue,
                                    };

                                    let side = match user_lookup.get_ids(&gamma_id) {
                                        Some(ids) => match ids.side_for_token(&o.asset_id) {
                                            Some(s) => s,
                                            None => continue,
                                        },
                                        None => continue,
                                    };

                                    let price = match o.price.to_string().parse::<Decimal>() {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };

                                    let event = OrderEvent {
                                        order_id: o.id.clone(),
                                        gamma_id,
                                        token_id: o.asset_id.clone(),
                                        side,
                                        price,
                                        size_matched: Decimal::from(size_matched),
                                        original_size: Decimal::ZERO,
                                        msg_type,
                                    };

                                    if order_tx.send(event).await.is_err() {
                                        warn!("Order event channel closed");
                                        return;
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
                    Err(e) => {
                        warn!("User events subscription failed: {}", e);
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }));

        info!("Started WS subscriptions for {} tokens, {} markets", token_ids.len(), gamma_ids.len());
    }
}