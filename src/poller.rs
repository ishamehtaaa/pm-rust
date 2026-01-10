use parking_lot::RwLock;
use polymarket_client_sdk::clob::types::OrderStatusType;
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use tokio::time::Duration;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;

type AuthenticatedClient = Client<Authenticated<Normal>>;

#[derive(Debug, Clone)]
struct TrackedOrder {
    order_id: String,
    market_id: String,
    side: MarketSide,
    original_size: Decimal,
    filled_size: Decimal,
    is_open: bool,
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

#[derive(Debug, Default, Clone)]
pub struct MarketPosition {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub pending_up: Decimal,
    pub pending_down: Decimal,
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
        let pos = self.positions.entry(market_id.clone()).or_default();
        match side {
            MarketSide::Up => pos.pending_up += size,
            MarketSide::Down => pos.pending_down += size,
        }

        self.tracked_orders.insert(
            order_id.clone(),
            TrackedOrder {
                order_id,
                market_id,
                side,
                original_size: size,
                filled_size: Decimal::ZERO,
                is_open: true,
            },
        );
    }

    fn reconcile_order(&mut self, order_id: &str, new_filled: Decimal, still_open: bool) {
        let Some(tracked) = self.tracked_orders.get_mut(order_id) else {
            return;
        };

        let fill_delta = new_filled - tracked.filled_size;
        if fill_delta > Decimal::ZERO {
            let pos = self.positions.entry(tracked.market_id.clone()).or_default();

            match tracked.side {
                MarketSide::Up => {
                    pos.up_shares += fill_delta;
                    pos.pending_up = (pos.pending_up - fill_delta).max(Decimal::ZERO);
                }
                MarketSide::Down => {
                    pos.down_shares += fill_delta;
                    pos.pending_down = (pos.pending_down - fill_delta).max(Decimal::ZERO);
                }
            }
            tracked.filled_size = new_filled;
        }

        if !still_open && tracked.is_open {
            tracked.is_open = false;
            let unfilled = tracked.original_size - tracked.filled_size;
            if unfilled > Decimal::ZERO {
                let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                match tracked.side {
                    MarketSide::Up => pos.pending_up = (pos.pending_up - unfilled).max(Decimal::ZERO),
                    MarketSide::Down => pos.pending_down = (pos.pending_down - unfilled).max(Decimal::ZERO),
                }
            }
        }
    }

    /// Reconcile all tracked orders against remote state
    pub fn reconcile_all(&mut self, remote_orders: &HashMap<String, OpenOrderResponse>) {
        let open_ids: Vec<(String, Decimal)> = self.tracked_orders
            .values()
            .filter(|o| o.is_open)
            .map(|o| (o.order_id.clone(), o.filled_size))
            .collect();

        for (order_id, current_filled) in open_ids {
            if let Some(remote) = remote_orders.get(&order_id) {
                let filled: Decimal = remote
                    .size_matched
                    .to_string()
                    .parse()
                    .unwrap_or_default();
                let still_open = matches!(remote.status, OrderStatusType::Live);
                self.reconcile_order(&order_id, filled, still_open);
            } else {
                // Order not in response = closed/cancelled
                self.reconcile_order(&order_id, current_filled, false);
            }
        }
    }

    pub fn effective_position(&self, market_id: &str) -> MarketPosition {
        self.positions.get(market_id).cloned().unwrap_or_default()
    }

    pub fn confirmed_position(&self, market_id: &str) -> (Decimal, Decimal) {
        self.positions
            .get(market_id)
            .map(|p| (p.up_shares, p.down_shares))
            .unwrap_or_default()
    }
}

pub fn spawn_order_poller(
    client: Arc<AuthenticatedClient>,
    ledger: Arc<RwLock<InventoryLedger>>,
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if let Err(e) = poll_all_orders(&client, &ledger).await {
                tracing::debug!(error = %e, "Order poll failed");
            }
        }
    })
}

async fn poll_all_orders(
    client: &AuthenticatedClient,
    ledger: &Arc<RwLock<InventoryLedger>>,
) -> anyhow::Result<()> {
    let req = OrdersRequest::builder().build();

    let mut all_orders: HashMap<String, OpenOrderResponse> = HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client.orders(&req, cursor).await?;
        for order in page.data {
            all_orders.insert(order.id.clone(), order);
        }
        if page.next_cursor == "LTE=" || page.count == 0 {
            break;
        }
        cursor = Some(page.next_cursor);
    }

    ledger.write().reconcile_all(&all_orders);

    Ok(())
}