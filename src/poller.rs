use futures_util::stream::{self, StreamExt};
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::OrderStatusType;
use polymarket_client_sdk::clob::types::request::OrdersRequest;
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::ws::types::response::OrderMessage;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::constants::{round_size, short_id};
use crate::ladder::OpenOrderInfo;

type AuthenticatedClient = Client<Authenticated<Normal>>;
type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

const MISSING_THRESHOLD: u32 = 3;
const ORDER_STATUS_CHECK_CONCURRENCY: usize = 4;

/// Messages sent to the ledger actor
#[derive(Debug)]
pub enum LedgerCommand {
    /// Record a new order we just placed
    OrderPlaced {
        order_id: String,
        market_id: String,
        token_id: String,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
    },
    /// WebSocket reported an order update
    OrderUpdate(OrderMessage),
    /// Mark orders as cancelled (from our cancel request)
    OrdersCancelled(Vec<String>),
    /// Set initial position from API
    SetInitialPosition {
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
    },
    /// Request current state (for reads)
    GetState {
        market_id: String,
        reply: tokio::sync::oneshot::Sender<LedgerSnapshot>,
    },
    /// HTTP reconciliation data
    Reconcile(HashMap<String, OpenOrderResponse>),
    /// Request orders missing from remote for multiple polls
    MissingCandidates {
        threshold: u32,
        reply: tokio::sync::oneshot::Sender<Vec<String>>,
    },
    /// Apply order status fetched directly by ID
    ApplyOrderStatus {
        order_id: String,
        filled_size: Option<Decimal>,
        still_open: Option<bool>,
    },
}

/// Snapshot of ledger state for a market
#[derive(Debug, Clone, Default)]
pub struct LedgerSnapshot {
    pub position: MarketPosition,
    pub pending_up: Decimal,
    pub pending_down: Decimal,
    pub open_orders: Vec<OpenOrderInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketSide {
    Up,
    Down,
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

/// The single source of truth for inventory.
///
/// All mutations go through the command channel, ensuring serialized access.
/// The WebSocket stream feeds updates directly here.
struct LedgerActor {
    positions: HashMap<String, MarketPosition>,
    tracked_orders: HashMap<String, TrackedOrder>,
    /// Maps token_id -> (market_id, side) for fast lookup from WS messages
    token_to_market: HashMap<String, (String, MarketSide)>,
}

impl LedgerActor {
    fn new() -> Self {
        Self {
            positions: HashMap::new(),
            tracked_orders: HashMap::new(),
            token_to_market: HashMap::new(),
        }
    }

    fn register_market(&mut self, market: &MarketTokens) {
        self.token_to_market.insert(
            market.up_token_id.clone(),
            (market.market_id.clone(), MarketSide::Up),
        );
        self.token_to_market.insert(
            market.down_token_id.clone(),
            (market.market_id.clone(), MarketSide::Down),
        );
    }

    fn handle_command(&mut self, cmd: LedgerCommand) {
        match cmd {
            LedgerCommand::OrderPlaced {
                order_id,
                market_id,
                token_id,
                side,
                size,
                price,
            } => {
                let rounded_size = round_size(size);
                debug!(
                    order_id = %short_id(&order_id, 8),
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

            LedgerCommand::OrderUpdate(msg) => {
                self.handle_order_message(msg);
            }

            LedgerCommand::OrdersCancelled(order_ids) => {
                for order_id in order_ids {
                    if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                        if tracked.is_open {
                            debug!(
                                order_id = %short_id(&order_id, 8),
                                side = ?tracked.side,
                                remaining = %(tracked.original_size - tracked.filled_size),
                                "Marking order cancelled"
                            );
                            tracked.is_open = false;
                        }
                    }
                }
            }

            LedgerCommand::SetInitialPosition {
                market_id,
                up_shares,
                down_shares,
            } => {
                debug!(
                    market_id = %market_id,
                    up_shares = %up_shares,
                    down_shares = %down_shares,
                    "Setting initial position"
                );
                let pos = self.positions.entry(market_id).or_default();
                pos.up_shares = up_shares;
                pos.down_shares = down_shares;
            }

            LedgerCommand::GetState { market_id, reply } => {
                let snapshot = self.get_snapshot(&market_id);
                let _ = reply.send(snapshot);
            }

            LedgerCommand::Reconcile(remote_orders) => {
                self.reconcile_with_remote(&remote_orders);
            }
            LedgerCommand::MissingCandidates { threshold, reply } => {
                let candidates = self
                    .tracked_orders
                    .values()
                    .filter(|o| o.is_open && o.missing_count >= threshold)
                    .map(|o| o.order_id.clone())
                    .collect();
                let _ = reply.send(candidates);
            }
            LedgerCommand::ApplyOrderStatus {
                order_id,
                filled_size,
                still_open,
            } => {
                if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                    if let Some(filled_normalized) = filled_size {
                        let fill_delta = filled_normalized - tracked.filled_size;

                        if fill_delta > Decimal::ZERO {
                            warn!(
                                order_id = %short_id(&order_id, 8),
                                fill_delta = %fill_delta,
                                "Catching up missed fill via order lookup"
                            );

                            let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                            match tracked.side {
                                MarketSide::Up => pos.up_shares += fill_delta,
                                MarketSide::Down => pos.down_shares += fill_delta,
                            }
                            tracked.filled_size = filled_normalized;
                        }
                    }

                    if let Some(is_open) = still_open {
                        if !is_open && tracked.is_open {
                            tracked.is_open = false;
                        }
                        if is_open {
                            tracked.missing_count = 0;
                        }
                    }
                }
            }
        }
    }

    /// Process a WebSocket order message - this is the real-time path
    fn handle_order_message(&mut self, msg: OrderMessage) {
        let order_id = &msg.id;

        // Try to find in our tracked orders first
        if let Some(tracked) = self.tracked_orders.get_mut(order_id) {
            tracked.missing_count = 0;
            // Normalize filled amount (API returns raw units, we track shares)
            if let Some(size_matched) = msg.size_matched {
                let new_filled_normalized = size_matched / dec!(1_000_000);
                let fill_delta = new_filled_normalized - tracked.filled_size;

                if fill_delta > Decimal::ZERO {
                    info!(
                        order_id = %short_id(order_id, 8),
                        fill_delta = %fill_delta,
                        total_filled = %new_filled_normalized,
                        side = ?tracked.side,
                        "Fill detected via WebSocket"
                    );

                    // Credit the position immediately
                    let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                    match tracked.side {
                        MarketSide::Up => pos.up_shares += fill_delta,
                        MarketSide::Down => pos.down_shares += fill_delta,
                    }
                    tracked.filled_size = new_filled_normalized;
                }
            }

            // Check if order is now closed
            let is_cancelled = matches!(msg.msg_type.as_deref(), Some("CANCELLATION"));
            let is_fully_matched = matches!(
                (msg.original_size, msg.size_matched),
                (Some(original), Some(matched)) if matched >= original
            );
            let is_closed = is_cancelled || is_fully_matched;

            if is_closed && tracked.is_open {
                debug!(
                    order_id = %short_id(order_id, 8),
                    msg_type = ?msg.msg_type,
                    "Order closed via WebSocket"
                );
                tracked.is_open = false;
            }
        } else {
            // Order not in our tracked set - might be from a previous session
            // or placed outside this bot. We can optionally track it.
            debug!(
                order_id = %short_id(order_id, 8),
                msg_type = ?msg.msg_type,
                "Received update for untracked order"
            );
        }
    }

    /// HTTP reconciliation - backup path to catch anything missed
    fn reconcile_with_remote(&mut self, remote_orders: &HashMap<String, OpenOrderResponse>) {
        let tracked_ids: Vec<String> = self
            .tracked_orders
            .values()
            .filter(|o| o.is_open)
            .map(|o| o.order_id.clone())
            .collect();

        for order_id in tracked_ids {
            if let Some(remote) = remote_orders.get(&order_id) {
                let filled: Decimal = remote.size_matched.to_string().parse().unwrap_or_default();
                let filled_normalized = filled / dec!(1_000_000);

                if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                    tracked.missing_count = 0;
                    let fill_delta = filled_normalized - tracked.filled_size;

                    if fill_delta > Decimal::ZERO {
                        // Missed a fill from WebSocket, catch up now
                        warn!(
                            order_id = %short_id(&order_id, 8),
                            fill_delta = %fill_delta,
                            "Catching up missed fill via HTTP reconciliation"
                        );

                        let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                        match tracked.side {
                            MarketSide::Up => pos.up_shares += fill_delta,
                            MarketSide::Down => pos.down_shares += fill_delta,
                        }
                        tracked.filled_size = filled_normalized;
                    }

                    let still_open = matches!(remote.status, OrderStatusType::Live);
                    if !still_open && tracked.is_open {
                        tracked.is_open = false;
                    }
                }
            } else {
                // Order not in remote - defer close until we confirm by ID
                if let Some(tracked) = self.tracked_orders.get_mut(&order_id) {
                    if tracked.is_open {
                        tracked.missing_count += 1;
                        debug!(
                            order_id = %short_id(&order_id, 8),
                            missing_count = tracked.missing_count,
                            "Order not in remote"
                        );
                    }
                }
            }
        }
    }

    fn get_snapshot(&self, market_id: &str) -> LedgerSnapshot {
        let position = self.positions.get(market_id).cloned().unwrap_or_default();

        let mut pending_up = Decimal::ZERO;
        let mut pending_down = Decimal::ZERO;
        let mut open_orders = Vec::new();

        for order in self.tracked_orders.values() {
            if order.is_open && order.market_id == market_id {
                let remaining = order.original_size - order.filled_size;
                match order.side {
                    MarketSide::Up => pending_up += remaining,
                    MarketSide::Down => pending_down += remaining,
                }
                open_orders.push(OpenOrderInfo {
                    order_id: order.order_id.clone(),
                    side: order.side,
                    price: order.price,
                    remaining_size: remaining,
                });
            }
        }

        LedgerSnapshot {
            position,
            pending_up,
            pending_down,
            open_orders,
        }
    }
}

/// Handle to interact with the ledger
#[derive(Clone)]
pub struct LedgerHandle {
    tx: mpsc::Sender<LedgerCommand>,
}

impl LedgerHandle {
    /// Record that we placed an order
    pub async fn record_order_placed(
        &self,
        order_id: String,
        market_id: String,
        token_id: String,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
    ) {
        let _ = self
            .tx
            .send(LedgerCommand::OrderPlaced {
                order_id,
                market_id,
                token_id,
                side,
                size,
                price,
            })
            .await;
    }

    /// Mark orders as cancelled
    pub async fn mark_orders_cancelled(&self, order_ids: Vec<String>) {
        let _ = self
            .tx
            .send(LedgerCommand::OrdersCancelled(order_ids))
            .await;
    }

    /// Set initial position from API
    pub async fn set_initial_position(
        &self,
        market_id: String,
        up_shares: Decimal,
        down_shares: Decimal,
    ) {
        let _ = self
            .tx
            .send(LedgerCommand::SetInitialPosition {
                market_id,
                up_shares,
                down_shares,
            })
            .await;
    }

    /// Get current state snapshot for a market
    pub async fn get_state(&self, market_id: &str) -> LedgerSnapshot {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let _ = self
            .tx
            .send(LedgerCommand::GetState {
                market_id: market_id.to_string(),
                reply: reply_tx,
            })
            .await;

        reply_rx.await.unwrap_or_default()
    }

    /// Send reconciliation data from HTTP poll
    pub async fn reconcile(&self, remote_orders: HashMap<String, OpenOrderResponse>) {
        let _ = self.tx.send(LedgerCommand::Reconcile(remote_orders)).await;
    }

    /// Get missing orders that need direct status checks
    pub async fn missing_candidates(&self, threshold: u32) -> Vec<String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let _ = self
            .tx
            .send(LedgerCommand::MissingCandidates {
                threshold,
                reply: reply_tx,
            })
            .await;
        reply_rx.await.unwrap_or_default()
    }

    /// Apply direct order status fetched by ID
    pub async fn apply_order_status(
        &self,
        order_id: String,
        filled_size: Option<Decimal>,
        still_open: Option<bool>,
    ) {
        let _ = self
            .tx
            .send(LedgerCommand::ApplyOrderStatus {
                order_id,
                filled_size,
                still_open,
            })
            .await;
    }
}

/// Spawn the ledger actor and return a handle
pub fn spawn_ledger_actor() -> LedgerHandle {
    let (tx, mut rx) = mpsc::channel::<LedgerCommand>(256);

    tokio::spawn(async move {
        let mut actor = LedgerActor::new();

        while let Some(cmd) = rx.recv().await {
            actor.handle_command(cmd);
        }
    });

    LedgerHandle { tx }
}

/// Spawn WebSocket order subscription that feeds into the ledger
pub fn spawn_order_feed(
    client: Arc<AuthenticatedWsClient>,
    ledger: LedgerHandle,
    market_ids: Vec<String>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if market_ids.is_empty() {
            warn!("No valid market IDs for order WebSocket subscription");
            return;
        }
        loop {
            info!(markets = ?market_ids, "Connecting to order WebSocket");

            match client.subscribe_orders(market_ids.clone()) {
                Ok(stream) => {
                    info!("Order WebSocket connected");

                    let mut stream = Box::pin(stream);
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(order_msg) => {
                                debug!(
                                    order_id = %short_id(&order_msg.id, 8),
                                    market = %order_msg.market,
                                    asset_id = %order_msg.asset_id,
                                    side = ?order_msg.side,
                                    price = %order_msg.price,
                                    original_size = ?order_msg.original_size,
                                    size_matched = ?order_msg.size_matched,
                                    msg_type = ?order_msg.msg_type,
                                    timestamp = ?order_msg.timestamp,
                                    msg_type = ?order_msg.msg_type,
                                    "Order update received"
                                );
                                let _ = ledger.tx.send(LedgerCommand::OrderUpdate(order_msg)).await;
                            }
                            Err(e) => {
                                error!(error = %e, "Order WebSocket error");
                                break;
                            }
                        }
                    }

                    warn!("Order WebSocket disconnected, reconnecting...");
                }
                Err(e) => {
                    error!(error = %e, "Failed to subscribe to orders");
                }
            }

            // Backoff before reconnect
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    })
}

/// Spawn HTTP reconciliation poller as backup
pub fn spawn_reconciliation_poller(
    client: Arc<AuthenticatedClient>,
    ledger: LedgerHandle,
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            match fetch_all_orders(&client).await {
                Ok(orders) => {
                    debug!(count = orders.len(), "HTTP reconciliation");
                    ledger.reconcile(orders).await;

                    let missing = ledger.missing_candidates(MISSING_THRESHOLD).await;
                    if !missing.is_empty() {
                        stream::iter(missing)
                            .map(|order_id| {
                                let client = client.clone();
                                let ledger = ledger.clone();
                                async move {
                                    match client.order(&order_id).await {
                                        Ok(order) => {
                                            let filled_raw: Decimal = order
                                                .size_matched
                                                .to_string()
                                                .parse()
                                                .unwrap_or_default();
                                            let filled_normalized = filled_raw / dec!(1_000_000);
                                            let still_open =
                                                matches!(order.status, OrderStatusType::Live);
                                            ledger
                                                .apply_order_status(
                                                    order_id,
                                                    Some(filled_normalized),
                                                    Some(still_open),
                                                )
                                                .await;
                                        }
                                        Err(e) => {
                                            debug!(
                                                order_id = %short_id(&order_id, 8),
                                                error = %e,
                                                "Order lookup failed"
                                            );
                                        }
                                    }
                                }
                            })
                            .buffer_unordered(ORDER_STATUS_CHECK_CONCURRENCY)
                            .collect::<Vec<_>>()
                            .await;
                    }
                }
                Err(e) => {
                    debug!(error = %e, "HTTP reconciliation failed");
                }
            }
        }
    })
}

async fn fetch_all_orders(
    client: &AuthenticatedClient,
) -> anyhow::Result<HashMap<String, OpenOrderResponse>> {
    let req = OrdersRequest::builder().build();
    let mut all_orders = HashMap::new();
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

    Ok(all_orders)
}
