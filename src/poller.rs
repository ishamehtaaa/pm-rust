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

    pub fn set_initial_position(
        &mut self,
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
    ) {
        let pos = self.positions.entry(market_id.clone()).or_default();
        pos.up_shares = up_shares;
        pos.down_shares = down_shares;

        info!(
            market = %self.market_label(&market_id),
            up_shares = %up_shares,
            down_shares = %down_shares,
            "Initial position set"
        );
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
            info!(
                order_id = %short_id(&order_id, 8),
                elapsed_ms,
                "Order update latency"
            );
            tracked.first_update_logged = true;
        }

        if let Some(size_matched) = msg.size_matched {
            let fill_delta = size_matched - tracked.filled_size;
            if fill_delta > Decimal::ZERO {
                let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                match tracked.side {
                    MarketSide::Up => pos.up_shares += fill_delta,
                    MarketSide::Down => pos.down_shares += fill_delta,
                }

                let market_id = tracked.market_id.clone();
                info!(
                    order_id = %short_id(&order_id, 8),
                    side = ?tracked.side,
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
