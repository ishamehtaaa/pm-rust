// poller.rs - WebSocket order updates (no notifications/trades)

use parking_lot::RwLock;
use futures::StreamExt;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::ws::types::response::{OrderMessage, OrderMessageType};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{debug, info, warn};

use crate::constants::short_id;
use crate::ladder::OpenOrderInfo;

type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketSide {
    Up,
    Down,
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    order_id: String,
    market_id: String,
    side: MarketSide,
    price: Decimal,
    original_size: Decimal,
    filled_size: Decimal,
    is_open: bool,
    placed_at: Instant,
    first_update_logged: bool,
}

#[derive(Debug, Default)]
pub struct InventoryLedger {
    positions: HashMap<String, MarketPosition>,
    tracked_orders: HashMap<String, TrackedOrder>,
    /// Maps market_id -> display label
    market_labels: HashMap<String, String>,
}

#[derive(Debug, Default, Clone)]
pub struct MarketPosition {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub pending_up: Decimal,
    pub pending_down: Decimal,
    /* Track actual cost paid for accurate profit calculation */
    pub up_cost: Decimal,
    pub down_cost: Decimal,
}

impl InventoryLedger {
    fn market_label(&self, market_id: &str) -> String {
        self.market_labels
            .get(market_id)
            .cloned()
            .unwrap_or_else(|| market_id.to_string())
    }

    /// Register a market label for logging.
    pub fn register_market(&mut self, market_id: String, label: String) {
        self.market_labels.insert(market_id, label);
    }

    pub fn unregister_market(&mut self, market_id: &str) {
        self.positions.remove(market_id);
        self.market_labels.remove(market_id);

        let removed_orders: Vec<String> = self
            .tracked_orders
            .iter()
            .filter(|(_, o)| o.market_id == market_id)
            .map(|(id, _)| id.clone())
            .collect();
        for order_id in removed_orders {
            self.tracked_orders.remove(&order_id);
        }
    }

    pub fn record_order_placed(
        &mut self,
        order_id: String,
        market_id: String,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
    ) {
        let market_label = self.market_label(&market_id);

        self.tracked_orders.insert(
            order_id.clone(),
            TrackedOrder {
                order_id,
                market_id: market_id.clone(),
                side,
                original_size: size,
                filled_size: Decimal::ZERO,
                price,
                is_open: true,
                placed_at: Instant::now(),
                first_update_logged: false,
            },
        );

        debug!(
            market = %market_label,
            side = ?side,
            size = %size,
            price = %price,
            "Order tracked"
        );
    }

    /// Update the position for a market from API data.
    /// Called both on initial discovery and periodic refreshes.
    /// 
    /// For pre-existing positions (shares > 0 but cost = 0), estimates cost
    /// using provided mid prices if available.
    pub fn sync_position(
        &mut self,
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
    ) {
        self.sync_position_with_prices(market_id, up_shares, down_shares, None, None);
    }
    
    /// Sync position with optional mid prices for estimating pre-existing costs.
    pub fn sync_position_with_prices(
        &mut self,
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
        up_mid: Option<Decimal>,
        down_mid: Option<Decimal>,
    ) {
        /* Get label before mutable borrow */
        let market_label = self.market_label(&market_id);
        
        let pos = self.positions.entry(market_id).or_default();
        let changed = pos.up_shares != up_shares || pos.down_shares != down_shares;
        
        /* 
         * For pre-existing shares (cost not yet tracked), estimate cost.
         * 
         * IMPORTANT: Only estimate on FIRST sync when pos.up_shares == 0.
         * This means the shares existed BEFORE the bot started.
         * 
         * We do NOT estimate for new fills during operation - the WebSocket
         * handler tracks actual fill prices. Estimating here would cause
         * double-counting (estimate + WS fill = too high).
         */
        let is_initial_sync = pos.up_shares == Decimal::ZERO && pos.down_shares == Decimal::ZERO;
        
        if is_initial_sync && up_shares > Decimal::ZERO && pos.up_cost == Decimal::ZERO {
            if let Some(mid) = up_mid {
                pos.up_cost = up_shares * mid;
                debug!(
                    market = %market_label,
                    shares = %up_shares,
                    est_cost = %pos.up_cost,
                    mid = %mid,
                    "Estimated Up cost for pre-existing position"
                );
            }
        }
        
        if is_initial_sync && down_shares > Decimal::ZERO && pos.down_cost == Decimal::ZERO {
            if let Some(mid) = down_mid {
                pos.down_cost = down_shares * mid;
                debug!(
                    market = %market_label,
                    shares = %down_shares,
                    est_cost = %pos.down_cost,
                    mid = %mid,
                    "Estimated Down cost for pre-existing position"
                );
            }
        }
        
        pos.up_shares = up_shares;
        pos.down_shares = down_shares;

        /* Only log if position actually changed */
        if changed {
            debug!(
                market = %market_label,
                up_shares = %up_shares,
                down_shares = %down_shares,
                "Position synced"
            );
        }
    }

    /// Process an order update from WebSocket.
    pub fn process_order_message(&mut self, msg: OrderMessage) {
        let order_id = msg.id;

        let Some(tracked) = self.tracked_orders.get_mut(&order_id) else {
            debug!(
                order_id = %short_id(&order_id, 8),
                msg_type = ?msg.msg_type,
                "Order update for untracked order"
            );
            return;
        };

        if !tracked.first_update_logged {
            let elapsed_ms = tracked.placed_at.elapsed().as_millis();
            debug!(
                order_id = %short_id(&order_id, 8),
                latency_ms = elapsed_ms,
                "First order update received"
            );
            tracked.first_update_logged = true;
        }

        if let Some(size_matched) = msg.size_matched {
            let fill_delta = size_matched - tracked.filled_size;
            if fill_delta > Decimal::ZERO {
                let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                
                /* Track fill value for accurate profit calculation */
                let fill_price = msg.price;
                let fill_value = fill_price * fill_delta;
                
                match tracked.side {
                    MarketSide::Up => {
                        pos.up_shares += fill_delta;
                        pos.up_cost += fill_value;
                    }
                    MarketSide::Down => {
                        pos.down_shares += fill_delta;
                        pos.down_cost += fill_value;
                    }
                }

                let market_id = tracked.market_id.clone();
                info!(
                    order_id = %short_id(&order_id, 8),
                    side = ?tracked.side,
                    fill_price = %fill_price,
                    fill_delta = %fill_delta,
                    total_filled = %size_matched,
                    market = %market_id,
                    "Fill detected via WebSocket"
                );

                tracked.filled_size = size_matched;
            }
        }

        let is_cancelled = matches!(msg.msg_type, Some(OrderMessageType::Cancellation));
        let is_fully_matched = matches!(
            (msg.original_size, msg.size_matched),
            (Some(original), Some(matched)) if matched >= original
        );
        let is_closed = is_cancelled || is_fully_matched;

        if is_closed && tracked.is_open {
            debug!(
                order_id = %short_id(&order_id, 8),
                msg_type = ?msg.msg_type,
                "Order closed via WebSocket"
            );
            tracked.is_open = false;
        }
    }

    pub fn effective_position(&self, market_id: &str) -> MarketPosition {
        self.positions
            .get(market_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn open_orders_for_market(&self, market_id: &str) -> Vec<OpenOrderInfo> {
        self.tracked_orders
            .values()
            .filter(|o| o.is_open && o.market_id == market_id)
            .map(|o| OpenOrderInfo {
                order_id: o.order_id.clone(),
                side: o.side,
                price: o.price,
                remaining_size: o.original_size - o.filled_size,
            })
            .collect()
    }

    pub fn mark_orders_cancelled(&mut self, order_ids: &[String]) {
        for order_id in order_ids {
            if let Some(tracked) = self.tracked_orders.get_mut(order_id) {
                if tracked.is_open {
                    tracked.is_open = false;
                    let market_id = tracked.market_id.clone();
                    debug!(
                        order_id = %short_id(order_id, 8),
                        market = %market_id,
                        "Order cancelled"
                    );
                }
            }
        }
    }

    pub fn apply_order_status(
        &mut self,
        order_id: &str,
        filled_size: Decimal,
        is_open: bool,
    ) {
        let Some(tracked) = self.tracked_orders.get_mut(order_id) else {
            debug!(
                order_id = %short_id(order_id, 8),
                "Order status for untracked order"
            );
            return;
        };

        let fill_delta = filled_size - tracked.filled_size;
        if fill_delta > Decimal::ZERO {
            let pos = self.positions.entry(tracked.market_id.clone()).or_default();
            match tracked.side {
                MarketSide::Up => pos.up_shares += fill_delta,
                MarketSide::Down => pos.down_shares += fill_delta,
            }

            debug!(
                order_id = %short_id(order_id, 8),
                side = ?tracked.side,
                fill_delta = %fill_delta,
                total_filled = %filled_size,
                "Reconciled fill via order status"
            );
            tracked.filled_size = filled_size;
        }

        tracked.is_open = is_open;
    }

    pub fn confirmed_position(&self, market_id: &str) -> (Decimal, Decimal) {
        self.positions
            .get(market_id)
            .map(|p| (p.up_shares, p.down_shares))
            .unwrap_or_default()
    }
    
    /*
     * Get average fill prices for each side.
     * Returns (avg_up_price, avg_down_price) or None if no fills on that side.
     */
    pub fn average_fill_prices(&self, market_id: &str) -> (Option<Decimal>, Option<Decimal>) {
        let Some(pos) = self.positions.get(market_id) else {
            return (None, None);
        };
        
        let avg_up = if pos.up_shares > Decimal::ZERO && pos.up_cost > Decimal::ZERO {
            Some(pos.up_cost / pos.up_shares)
        } else {
            None
        };
        
        let avg_down = if pos.down_shares > Decimal::ZERO && pos.down_cost > Decimal::ZERO {
            Some(pos.down_cost / pos.down_shares)
        } else {
            None
        };
        
        (avg_up, avg_down)
    }
    
    /*
     * Calculate maximum price we can pay for incomplete side and still profit.
     * If we have Up at 30¢ average, we can pay up to (100¢ - 30¢ - margin) = ~68¢ for Down.
     * Returns (max_up_price, max_down_price) - None means no position on that side yet.
     */
    pub fn max_completion_prices(&self, market_id: &str, profit_margin: Decimal) -> (Option<Decimal>, Option<Decimal>) {
        let (avg_up, avg_down) = self.average_fill_prices(market_id);
        
        /* If we have Up, calculate max Down price */
        let max_down = avg_up.map(|up_avg| {
            (Decimal::ONE - up_avg - profit_margin).max(Decimal::ZERO)
        });
        
        /* If we have Down, calculate max Up price */
        let max_up = avg_down.map(|down_avg| {
            (Decimal::ONE - down_avg - profit_margin).max(Decimal::ZERO)
        });
        
        (max_up, max_down)
    }
}

/// Spawn WebSocket order feed that updates the ledger.
pub fn spawn_order_feed(
    client: Arc<AuthenticatedWsClient>,
    ledger: Arc<RwLock<InventoryLedger>>,
    market_ids: Vec<polymarket_client_sdk::types::B256>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if market_ids.is_empty() {
            warn!("No valid markets for order WebSocket subscription");
            return;
        }

        loop {
            info!(markets = ?market_ids, "Connecting order WebSocket");

            match client.subscribe_orders(market_ids.clone()) {
                Ok(stream) => {
                    info!("Order WebSocket connected");
                    let mut stream = Box::pin(stream);

                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(order_msg) => {
                                ledger.write().process_order_message(order_msg);
                            }
                            Err(e) => {
                                warn!(error = %e, "Order WebSocket error");
                                break;
                            }
                        }
                    }

                    warn!("Order WebSocket disconnected, reconnecting");
                }
                Err(e) => {
                    warn!(error = %e, "Failed to subscribe to order WebSocket");
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;  /* Reduced from 5s for faster reconnect */
        }
    })
}
