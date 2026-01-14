// poller.rs - NOTIFICATION-BASED VERSION
// Drop-in replacement - same public API, different implementation

use parking_lot::RwLock;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::response::NotificationPayload;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;
use tracing::{debug, info, warn, trace};
use chrono::Utc;

use crate::ladder::OpenOrderInfo;

type AuthenticatedClient = Client<Authenticated<Normal>>;

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
}

#[derive(Debug, Default)]
pub struct InventoryLedger {
    positions: HashMap<String, MarketPosition>,
    tracked_orders: HashMap<String, TrackedOrder>,
    /// Maps asset_id -> (market_id, side) for quick notification processing
    asset_to_market: HashMap<String, (String, MarketSide)>,
    /// Maps market_id -> display label
    market_labels: HashMap<String, String>,
    /// Throttle unregistered asset logs
    last_unregistered_asset_log: HashMap<String, Instant>,
    /// Track processed notification IDs to avoid duplicates (order_id -> last_matched_size)
    processed_notifications: HashMap<String, Decimal>,
}

#[derive(Debug, Default, Clone)]
pub struct MarketPosition {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub pending_up: Decimal,   // Kept for compatibility but not used in calculations
    pub pending_down: Decimal, // Kept for compatibility but not used in calculations
}

impl InventoryLedger {
    fn should_log_unregistered_asset(&mut self, asset_id: &str) -> bool {
        let now = Instant::now();
        let min_interval = Duration::from_secs(10);
        match self.last_unregistered_asset_log.get(asset_id) {
            Some(last) if now.duration_since(*last) < min_interval => false,
            _ => {
                self.last_unregistered_asset_log
                    .insert(asset_id.to_string(), now);
                true
            }
        }
    }

    fn market_label(&self, market_id: &str) -> String {
        self.market_labels
            .get(market_id)
            .cloned()
            .unwrap_or_else(|| market_id.to_string())
    }

    /// Register a market so notifications can be routed correctly
    pub fn register_market(
        &mut self,
        market_id: String,
        up_token_id: String,
        down_token_id: String,
        label: String,
    ) {
        let market_label = label.clone();
        debug!(
            market = %market_label,
            up_token = %up_token_id,
            down_token = %down_token_id,
            "Registering market tokens"
        );

        self.market_labels.insert(market_id.clone(), label);
        self.asset_to_market.insert(
            up_token_id,
            (market_id.clone(), MarketSide::Up),
        );
        self.asset_to_market.insert(
            down_token_id,
            (market_id.clone(), MarketSide::Down),
        );
    }

    pub fn unregister_market(&mut self, market_id: &str, up_token_id: &str, down_token_id: &str) {
        self.positions.remove(market_id);
        self.market_labels.remove(market_id);
        self.asset_to_market.remove(up_token_id);
        self.asset_to_market.remove(down_token_id);

        let removed_orders: Vec<String> = self
            .tracked_orders
            .iter()
            .filter(|(_, o)| o.market_id == market_id)
            .map(|(id, _)| id.clone())
            .collect();
        for order_id in removed_orders {
            self.tracked_orders.remove(&order_id);
            self.processed_notifications.remove(&order_id);
        }
    }

    pub fn record_order_placed(
        &mut self,
        order_id: String,
        market_id: String,
        token_id: String,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
    ) {
        let market_label = self.market_label(&market_id);
        // Register token_id -> (market_id, side) if not already registered
        self.asset_to_market.entry(token_id.clone())
            .or_insert((market_id.clone(), side));

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
            },
        );

        debug!(
            market = %market_label,
            token_id = %token_id,
            side = ?side,
            size = %size,
            price = %price,
            "Order tracked (waiting for notification)"
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

    /// Process a notification from the API
    pub fn process_notification(&mut self, notif: &NotificationPayload) {
        let order_id = &notif.order_id;

        // Parse notification sizes
        let matched = notif.matched_size;
        let remaining = notif.remaining_size;
        let original = notif.original_size;

        // Check if we've already processed this exact notification state
        if let Some(&last_matched) = self.processed_notifications.get(order_id) {
            if last_matched == matched {
                // Already processed this notification, skip
                debug!(
                    order_id = %order_id,
                    matched = %matched,
                    "Skipping duplicate notification"
                );
                return;
            }
        }

        // Record that we've processed this notification state
        self.processed_notifications.insert(order_id.clone(), matched);

        // Determine market_id and side from asset_id
        let asset_id = notif.asset_id.to_string();
        let (market_id, side) = match self.asset_to_market.get(&asset_id) {
            Some((m, s)) => (m.clone(), *s),
            None => {
                if self.should_log_unregistered_asset(&asset_id) {
                    trace!(
                        asset_id = %asset_id,
                        "Notification for unregistered asset"
                    );
                }
                return;
            }
        };

        let price = notif.price;

        let market_label = self.market_label(&market_id);
        debug!(
            order_id = %order_id,
            outcome = %notif.outcome,
            matched = %matched,
            remaining = %remaining,
            side = ?side,
            market = %market_label,
            "📬 Fill notification"
        );

        // Get or create tracked order
        let tracked = self.tracked_orders
            .entry(order_id.clone())
            .or_insert_with(|| TrackedOrder {
                order_id: order_id.clone(),
                market_id: market_id.clone(),
                side,
                price,
                original_size: original,
                filled_size: Decimal::ZERO,
                is_open: true,
            });

        // Calculate new fills
        let old_filled = tracked.filled_size;
        let new_filled = matched;
        let fill_delta = new_filled - old_filled;

        if fill_delta > Decimal::ZERO {
            // Update position
            let pos = self.positions.entry(market_id.clone()).or_default();
            
            match side {
                MarketSide::Up => pos.up_shares += fill_delta,
                MarketSide::Down => pos.down_shares += fill_delta,
            }

            debug!(
                order_id = %order_id,
                side = ?side,
                fill_delta = %fill_delta,
                total_filled = %new_filled,
                up_shares = %pos.up_shares,
                down_shares = %pos.down_shares,
                market = %market_label,
                "✅ Position updated"
            );
        }

        // Update order state using remaining_size
        tracked.filled_size = new_filled;
        tracked.is_open = remaining > Decimal::ZERO;

        if !tracked.is_open {
            debug!(
                order_id = %order_id,
                filled = %tracked.filled_size,
                original = %tracked.original_size,
                market = %market_label,
                "Order fully filled/closed"
            );
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
            let mut cancelled = None;
            if let Some(tracked) = self.tracked_orders.get_mut(order_id) {
                if tracked.is_open {
                    tracked.is_open = false;
                    cancelled = Some((
                        tracked.market_id.clone(),
                        tracked.filled_size,
                        tracked.original_size,
                    ));
                }
            }
            if let Some((market_id, filled, original)) = cancelled {
                let market_label = self.market_label(&market_id);
                debug!(
                    order_id = %order_id,
                    filled = %filled,
                    original = %original,
                    market = %market_label,
                    "Order cancelled"
                );
            }
        }
    }

    pub fn confirmed_position(&self, market_id: &str) -> (Decimal, Decimal) {
        self.positions
            .get(market_id)
            .map(|p| (p.up_shares, p.down_shares))
            .unwrap_or_default()
    }
}

/// Spawn notification poller that fetches from API periodically
pub fn spawn_order_poller(
    client: Arc<AuthenticatedClient>,
    ledger: Arc<RwLock<InventoryLedger>>,
    _token_pairs: Arc<RwLock<Vec<(String, String)>>>, // Unused now, kept for compatibility
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("Notification & trades poller started (interval: {:?})", poll_interval);

        loop {
            interval.tick().await;

            // Fetch notifications
            let notification_count = match client.notifications().await {
                Ok(notifications) => {
                    let count = notifications.len();
                    if count > 0 {
                        debug!("📬 Processing {} notifications", count);
                        let mut ledger_write = ledger.write();
                        for notif in notifications {
                            ledger_write.process_notification(&notif.payload);
                        }
                    }
                    count
                }
                Err(e) => {
                    warn!(error = %e, "Failed to fetch notifications");
                    0
                }
            };

            // Fetch recent trades for comparison
            if let Err(e) = fetch_and_log_trades(&client, &ledger).await {
                warn!(error = %e, "Failed to fetch trades");
            }

            if notification_count == 0 {
                trace!("No new notifications or trades");
            }
        }
    })
}

async fn fetch_and_log_trades(
    client: &AuthenticatedClient,
    ledger: &Arc<RwLock<InventoryLedger>>,
) -> anyhow::Result<()> {
    use polymarket_client_sdk::clob::types::request::TradesRequest;

    // Fetch trades from last 60 seconds
    let after = (Utc::now().timestamp() - 60) * 1000; // milliseconds
    
    let request = TradesRequest::builder()
        .after(after)
        .build();

    let page = client.trades(&request, None).await?;
    
    if page.data.is_empty() {
        return Ok(());
    }

    info!("🔄 Fetched {} recent trades for comparison", page.data.len());

    // Build a map of our tracked orders for quick lookup
        let tracked_map: HashMap<String, (Decimal, Decimal, MarketSide)> = {
            let ledger_read = ledger.read();
            ledger_read.tracked_orders
                .iter()
                .filter(|(_, o)| o.is_open)
                .map(|(id, o)| (id.clone(), (o.filled_size, o.original_size, o.side)))
                .collect()
        };

    if tracked_map.is_empty() {
        return Ok(());
    }

    for trade in &page.data {
        // Check if this trade is for one of our tracked orders
        if let Some((known_filled, original_size, side)) = tracked_map.get(&trade.taker_order_id) {
            let trade_size: Decimal = trade.size;
            let price: Decimal = trade.price;
            
            info!("🔍 TRADE COMPARISON for order {}", trade.taker_order_id);
            info!("   Side: {:?} | Price: {}", side, price);
            info!("   Trade API shows: size={}", trade_size);
            info!("   Our ledger has: filled={}/{}", known_filled, original_size);
            
            if trade_size > *known_filled {
                warn!(
                    "⚠️  DISCREPANCY: Trade API shows {} but we only have {} filled - missing {}",
                    trade_size,
                    known_filled,
                    trade_size - known_filled
                );
            } else if trade_size == *known_filled {
                debug!("✅ Trade data matches our ledger");
            }
        }
    }

    Ok(())
}
