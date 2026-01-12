use parking_lot::RwLock;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::OrderStatusType;
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{debug, error};

use crate::clob_api::get_trades;
use crate::constants::{round_size, short_id};
use crate::ladder::OpenOrderInfo;

type AuthenticatedClient = Client<Authenticated<Normal>>;

/* We check if an order is in the remote order book this many times before marking the order as closed. */
const MISSING_THRESHOLD: u32 = 3;

#[derive(Debug, Clone)]
struct TrackedOrder {
    order_id: String,
    token_id: String,
    market_id: String,
    side: MarketSide,
    price: Decimal,
    original_size: Decimal,
    filled_size: Decimal,
    is_open: bool,
    missing_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketSide {
    Up,
    Down,
}

#[derive(Debug, Default)]
pub struct InventoryLedger {
    positions: HashMap<String, MarketPosition>,
    tracked_orders: HashMap<String, TrackedOrder>,
}

#[derive(Debug, Clone)]
pub struct MarketTokens {
    pub market_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
}

#[derive(Debug, Default, Clone)]
pub struct MarketPosition {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
}

impl InventoryLedger {
    pub fn record_order_placed(
        &mut self,
        order_id: String,
        market_id: String,
        token_id: String,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
    ) {
        let rounded_size = round_size(size);

        debug!(
            order_id = %order_id,
            market_id = %market_id,
            side = ?side,
            size = %rounded_size,
            "Recording order placed"
        );

        self.tracked_orders.insert(
            order_id.clone(),
            TrackedOrder {
                order_id,
                token_id,
                market_id,
                side,
                original_size: rounded_size,
                filled_size: Decimal::ZERO,
                price,
                is_open: true,
                missing_count: 0,
            },
        );
    }

    pub fn set_initial_position(
        &mut self,
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
    ) {
        debug!(
            market_id = %market_id,
            up_shares = %up_shares,
            down_shares = %down_shares,
            "set_initial_position called with"
        );

        let pos = self.positions.entry(market_id).or_default();
        pos.up_shares = up_shares;
        pos.down_shares = down_shares;
    }
    pub fn effective_position(&self, market_id: &str) -> (MarketPosition, Decimal, Decimal) {
        let pos = self.confirmed_position(market_id);
        let (pending_up, pending_down) = self.pending_for_market(market_id);
        (pos, pending_up, pending_down)
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
                    debug!(
                        order_id = %order_id,
                        side = ?tracked.side,
                        remaining = %(tracked.original_size - tracked.filled_size),
                        "Marking order cancelled"
                    );
                    tracked.is_open = false;
                }
            }
        }
    }

    fn reconcile_order(&mut self, order_id: &str, new_filled: Decimal, still_open: bool) {
        let Some(tracked) = self.tracked_orders.get_mut(order_id) else {
            return;
        };

        // Normalize filled amount from API (raw units to shares)
        let new_filled_normalized = new_filled / dec!(1_000_000);

        let fill_delta = new_filled_normalized - tracked.filled_size;

        if fill_delta > Decimal::ZERO {
            debug!(
                order_id = %order_id,
                fill_delta = %fill_delta,
                side = ?tracked.side,
                "Fill detected"
            );

            let pos = self.positions.entry(tracked.market_id.clone()).or_default();
            match tracked.side {
                MarketSide::Up => pos.up_shares += fill_delta,
                MarketSide::Down => pos.down_shares += fill_delta,
            }
            tracked.filled_size = new_filled_normalized;
        }

        if !still_open && tracked.is_open {
            debug!(
                order_id = %order_id,
                "Order closed"
            );
            tracked.is_open = false;
        }
    }

    /* Reconcile all orders against the books on Polymarket. */
    pub fn reconcile_all(&mut self, remote_orders: &HashMap<String, OpenOrderResponse>) {
        for (id, tracked) in &self.tracked_orders {
            if tracked.is_open {
                let in_remote = remote_orders.contains_key(id);
                debug!(
                    order_id = %short_id(id, 8),
                    market_id = %tracked.market_id,
                    in_remote = in_remote,
                    missing_count = tracked.missing_count,
                    "Tracked order status"
                );
            }
        }

        // Log all remote orders
        for (id, remote) in remote_orders {
            let in_tracked = self.tracked_orders.contains_key(id);
            debug!(
                order_id = %short_id(id, 8),
                status = ?remote.status,
                in_tracked = in_tracked,
                "Remote order status"
            );
        }
        let open_ids: Vec<(String, Decimal)> = self
            .tracked_orders
            .values()
            .filter(|o| o.is_open)
            .map(|o| (o.order_id.clone(), o.filled_size))
            .collect();

        debug!(
            tracked_open_count = open_ids.len(),
            remote_count = remote_orders.len(),
            "Starting reconciliation"
        );

        for (order_id, current_filled) in open_ids {
            if let Some(remote) = remote_orders.get(&order_id) {
                // Order found in remote — reset missing count
                if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                    tracked.missing_count = 0;
                }

                let filled: Decimal = remote.size_matched.to_string().parse().unwrap_or_default();
                let still_open = matches!(remote.status, OrderStatusType::Live);

                debug!(
                    order_id = %order_id,
                    current_filled = %current_filled,
                    remote_filled = %filled,
                    still_open,
                    "Reconciling order"
                );

                self.reconcile_order(&order_id, filled, still_open);
            } else {
                // Order not found — increment missing count
                let should_close = if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                    tracked.missing_count += 1;
                    debug!(
                        order_id = %order_id,
                        missing_count = tracked.missing_count,
                        threshold = MISSING_THRESHOLD,
                        "Order not in remote"
                    );
                    tracked.missing_count >= MISSING_THRESHOLD
                } else {
                    false
                };

                if should_close {
                    debug!(
                        order_id = %order_id,
                        "Order missing {} times, marking closed",
                        MISSING_THRESHOLD
                    );
                    self.reconcile_order(&order_id, current_filled, false);
                }
            }
        }
    }
    pub fn confirmed_position(&self, market_id: &str) -> MarketPosition {
        self.positions.get(market_id).cloned().unwrap_or_default()
    }

    pub fn pending_for_market(&self, market_id: &str) -> (Decimal, Decimal) {
        let mut pending_up = Decimal::ZERO;
        let mut pending_down = Decimal::ZERO;

        for order in self.tracked_orders.values() {
            if order.is_open && order.market_id == market_id {
                let remaining = order.original_size - order.filled_size;
                match order.side {
                    MarketSide::Up => pending_up += remaining,
                    MarketSide::Down => pending_down += remaining,
                }
            }
        }

        (pending_up, pending_down)
    }
}

pub fn spawn_order_poller(
    client: Arc<AuthenticatedClient>,
    ledger: Arc<RwLock<InventoryLedger>>,
    market_tokens: Arc<RwLock<Vec<MarketTokens>>>,
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let markets = market_tokens.read().clone();

            for market in markets {
                if let Err(e) = poll_market_orders(
                    &client,
                    &ledger,
                    &market.market_id,
                    &market.up_token_id,
                    &market.down_token_id,
                )
                .await
                {
                    tracing::debug!(market_id = %market.market_id, error = %e, "Order poll failed");
                }
            }
        }
    })
}

async fn poll_market_orders(
    client: &AuthenticatedClient,
    ledger: &Arc<RwLock<InventoryLedger>>,
    market_id: &str,
    up_token_id: &str,
    down_token_id: &str,
) -> anyhow::Result<()> {
    let req = OrdersRequest::builder().build();

    match client.orders(&req, None).await {
        Ok(page) => {
            debug!(
                count = page.data.len(),
                next_cursor = %page.next_cursor,
                "Raw orders response"
            );
        }
        Err(e) => {
            error!(error = %e, "Polling orders failed.");
        }
    }

    let mut all_orders: HashMap<String, OpenOrderResponse> = HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        debug!(market_id = %market_id, cursor = ?cursor, "Fetching orders page");

        let page = match client.orders(&req, cursor).await {
            Ok(p) => p,
            Err(e) => {
                error!(market_id = %market_id, error = %e, "Failed to fetch orders");
                return Err(e.into());
            }
        };

        debug!(
            market_id = %market_id,
            page_count = page.data.len(),
            next_cursor = %page.next_cursor,
            "Got orders page"
        );

        for order in page.data {
            all_orders.insert(order.id.clone(), order);
        }

        if page.next_cursor == "LTE=" || page.count == 0 {
            break;
        }
        cursor = Some(page.next_cursor);
    }

    debug!(
        market_id = %market_id,
        total_orders = all_orders.len(),
        "Calling reconcile_all"
    );

    ledger.write().reconcile_all(&all_orders);

    Ok(())
}
