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
    /// The paired order on the opposite side (for pair execution protection)
    paired_order_id: Option<String>,
    /// Actual fill price (may differ from placed price for limit orders)
    actual_fill_price: Option<Decimal>,
}

/// Information about a fill that may require canceling the paired order
#[derive(Debug, Clone)]
pub struct PairFillEvent {
    pub filled_order_id: String,
    pub paired_order_id: String,
    pub market_id: String,
    pub filled_side: MarketSide,
    pub filled_price: Decimal,
    pub filled_size: Decimal,
}

#[derive(Debug, Default)]
pub struct InventoryLedger {
    positions: HashMap<String, MarketPosition>,
    tracked_orders: HashMap<String, TrackedOrder>,
    /// Maps market_id -> display label
    market_labels: HashMap<String, String>,
    /// Pending pair fill events that need to be processed by the bot
    pending_pair_fills: Vec<PairFillEvent>,
}

#[derive(Debug, Default, Clone)]
pub struct MarketPosition {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub pending_up: Decimal,
    pub pending_down: Decimal,
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
                paired_order_id: None,
                actual_fill_price: None,
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

    /// Link two orders as a pair (for pair execution protection)
    pub fn link_paired_orders(&mut self, order_id_a: &str, order_id_b: &str) {
        if let Some(order_a) = self.tracked_orders.get_mut(order_id_a) {
            order_a.paired_order_id = Some(order_id_b.to_string());
        }
        if let Some(order_b) = self.tracked_orders.get_mut(order_id_b) {
            order_b.paired_order_id = Some(order_id_a.to_string());
        }
    }

    /// Get pending pair fill events and clear them
    pub fn take_pending_pair_fills(&mut self) -> Vec<PairFillEvent> {
        std::mem::take(&mut self.pending_pair_fills)
    }

    /// Get the filled price for an order (for P&L tracking)
    pub fn get_fill_price(&self, order_id: &str) -> Option<Decimal> {
        self.tracked_orders
            .get(order_id)
            .and_then(|o| o.actual_fill_price.or(Some(o.price)))
    }

    /// Get the total filled size for an order (0 if unknown/untracked).
    pub fn filled_size(&self, order_id: &str) -> Decimal {
        self.tracked_orders
            .get(order_id)
            .map(|o| o.filled_size)
            .unwrap_or_default()
    }

    /// Get the order's limit price (if tracked).
    pub fn order_price(&self, order_id: &str) -> Option<Decimal> {
        self.tracked_orders.get(order_id).map(|o| o.price)
    }

    /// Check if paired order is still open
    pub fn is_paired_order_open(&self, order_id: &str) -> Option<bool> {
        self.tracked_orders
            .get(order_id)
            .and_then(|o| o.paired_order_id.as_ref())
            .and_then(|paired_id| self.tracked_orders.get(paired_id))
            .map(|paired| paired.is_open)
    }

    /// Update the position for a market from API data.
    /// Called both on initial discovery and periodic refreshes.
    pub fn sync_position(
        &mut self,
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
    ) {
        let pos = self.positions.entry(market_id.clone()).or_default();
        let changed = pos.up_shares != up_shares || pos.down_shares != down_shares;
        pos.up_shares = up_shares;
        pos.down_shares = down_shares;

        // Only log if position actually changed
        if changed {
            debug!(
                market = %self.market_label(&market_id),
                up_shares = %up_shares,
                down_shares = %down_shares,
                "Position synced"
            );
        }
    }

    /// Process an order update from WebSocket.
    pub fn process_order_message(&mut self, msg: OrderMessage) {
        let order_id = msg.id.clone();

        // First pass: gather info about the tracked order without holding a mutable borrow
        let tracked_info = {
            let Some(tracked) = self.tracked_orders.get(&order_id) else {
                debug!(
                    order_id = %short_id(&order_id, 8),
                    msg_type = ?msg.msg_type,
                    "Order update for untracked order"
                );
                return;
            };
            
            (
                tracked.first_update_logged,
                tracked.placed_at,
                tracked.price,
                tracked.actual_fill_price,
                tracked.filled_size,
                tracked.market_id.clone(),
                tracked.side,
                tracked.paired_order_id.clone(),
                tracked.is_open,
            )
        };
        
        let (first_update_logged, placed_at, price, actual_fill_price, 
             prev_filled_size, market_id, side, paired_order_id, was_open) = tracked_info;

        // Log first update
        if !first_update_logged {
            let elapsed_ms = placed_at.elapsed().as_millis();
            debug!(
                order_id = %short_id(&order_id, 8),
                latency_ms = elapsed_ms,
                "First order update received"
            );
            if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                tracked.first_update_logged = true;
            }
        }

        // For limit orders, fills happen at the order price or better.
        // We track the placed price as the fill price (conservative estimate).
        // Note: Polymarket limit orders always fill at exactly the limit price.
        let fill_price = actual_fill_price.unwrap_or(price);
        if actual_fill_price.is_none() && msg.size_matched.is_some() {
            if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                tracked.actual_fill_price = Some(price);
            }
        }

        let mut pair_fill_event: Option<PairFillEvent> = None;

        if let Some(size_matched) = msg.size_matched {
            let fill_delta = size_matched - prev_filled_size;
            if fill_delta > Decimal::ZERO {
                let pos = self.positions.entry(market_id.clone()).or_default();
                match side {
                    MarketSide::Up => pos.up_shares += fill_delta,
                    MarketSide::Down => pos.down_shares += fill_delta,
                }

                info!(
                    order_id = %short_id(&order_id, 8),
                    side = ?side,
                    fill_delta = %fill_delta,
                    total_filled = %size_matched,
                    fill_price = %fill_price,
                    market = %market_id,
                    "Fill detected via WebSocket"
                );

                // Check if paired order is still open
                if let Some(ref paired_id) = paired_order_id {
                    let paired_is_open = self.tracked_orders
                        .get(paired_id)
                        .map(|p| p.is_open)
                        .unwrap_or(false);
                    
                    if paired_is_open {
                        pair_fill_event = Some(PairFillEvent {
                            filled_order_id: order_id.clone(),
                            paired_order_id: paired_id.clone(),
                            market_id: market_id.clone(),
                            filled_side: side,
                            filled_price: fill_price,
                            filled_size: fill_delta,
                        });
                    }
                }

                // Update filled size
                if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                    tracked.filled_size = size_matched;
                }
            }
        }

        // Queue the pair fill event
        if let Some(event) = pair_fill_event {
            self.pending_pair_fills.push(event);
        }

        let is_cancelled = matches!(msg.msg_type, Some(OrderMessageType::Cancellation));
        let is_fully_matched = matches!(
            (msg.original_size, msg.size_matched),
            (Some(original), Some(matched)) if matched >= original
        );
        let is_closed = is_cancelled || is_fully_matched;

        if is_closed && was_open {
            debug!(
                order_id = %short_id(&order_id, 8),
                msg_type = ?msg.msg_type,
                "Order closed via WebSocket"
            );
            if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                tracked.is_open = false;
            }
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

            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
}
