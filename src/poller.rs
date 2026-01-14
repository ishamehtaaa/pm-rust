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
use crate::ladder::{OpenOrderInfo, OrderSide};

type AuthenticatedWsClient = WsClient<Authenticated<Normal>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketSide {
    Up,
    Down,
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    order_id: String,
    market_id: String,
    side: MarketSide,
    order_side: OrderSide,
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
    token_to_market: HashMap<String, (String, MarketSide)>,
    market_cash: HashMap<String, Decimal>,
    pnl_base: HashMap<String, Decimal>,
    buy_fills: HashMap<(String, MarketSide), bool>,
    cost_basis: HashMap<(String, MarketSide), CostBasis>,
}

#[derive(Debug, Clone, Copy, Default)]
struct CostBasis {
    qty: Decimal,
    cost: Decimal,
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

    pub fn register_market_tokens(
        &mut self,
        market_id: String,
        up_token_id: String,
        down_token_id: String,
    ) {
        self.token_to_market
            .insert(up_token_id, (market_id.clone(), MarketSide::Up));
        self.token_to_market
            .insert(down_token_id, (market_id, MarketSide::Down));
    }

    pub fn unregister_market(&mut self, market_id: &str) {
        self.positions.remove(market_id);
        self.market_labels.remove(market_id);
        self.market_cash.remove(market_id);
        self.pnl_base.remove(market_id);
        self.cost_basis
            .retain(|(m, _), _| m != market_id);
        self.buy_fills
            .retain(|(m, _), _| m != market_id);
        self.token_to_market
            .retain(|_, (m, _)| m != market_id);

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
        order_side: OrderSide,
        size: Decimal,
        price: Decimal,
    ) {
        let market_label = self.market_label(&market_id);

        if self.tracked_orders.contains_key(&order_id) {
            debug!(
                order_id = %short_id(&order_id, 8),
                market = %market_label,
                "Order already tracked"
            );
            return;
        }

        self.tracked_orders.insert(
            order_id.clone(),
            TrackedOrder {
                order_id: order_id.clone(),
                market_id: market_id.clone(),
                side,
                order_side,
                original_size: size,
                filled_size: Decimal::ZERO,
                price,
                is_open: true,
                placed_at: Instant::now(),
                first_update_logged: false,
            },
        );

        debug!(
            order_id = %short_id(&order_id, 8),
            market = %market_label,
            side = ?side,
            order_side = ?order_side,
            size = %size,
            price = %price,
            "Order tracked"
        );
    }

    pub fn has_buy_fill(&self, market_id: &str, side: MarketSide) -> bool {
        self.buy_fills
            .get(&(market_id.to_string(), side))
            .copied()
            .unwrap_or(false)
    }

    pub fn projected_pair_avg_cost(
        &self,
        market_id: &str,
        side: MarketSide,
        add_qty: Decimal,
        add_price: Decimal,
    ) -> Option<Decimal> {
        let up = self.cost_basis.get(&(market_id.to_string(), MarketSide::Up));
        let down = self
            .cost_basis
            .get(&(market_id.to_string(), MarketSide::Down));

        let (up_qty, up_cost) = up
            .map(|c| (c.qty, c.cost))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));
        let (down_qty, down_cost) = down
            .map(|c| (c.qty, c.cost))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));

        let (proj_up_avg, proj_down_avg) = match side {
            MarketSide::Up => {
                let total_qty = up_qty + add_qty;
                let total_cost = up_cost + add_price * add_qty;
                let avg_up = if total_qty > Decimal::ZERO {
                    total_cost / total_qty
                } else {
                    Decimal::ZERO
                };
                let avg_down = if down_qty > Decimal::ZERO && down_cost > Decimal::ZERO {
                    down_cost / down_qty
                } else {
                    Decimal::ZERO
                };
                (avg_up, avg_down)
            }
            MarketSide::Down => {
                let total_qty = down_qty + add_qty;
                let total_cost = down_cost + add_price * add_qty;
                let avg_down = if total_qty > Decimal::ZERO {
                    total_cost / total_qty
                } else {
                    Decimal::ZERO
                };
                let avg_up = if up_qty > Decimal::ZERO && up_cost > Decimal::ZERO {
                    up_cost / up_qty
                } else {
                    Decimal::ZERO
                };
                (avg_up, avg_down)
            }
        };

        Some(proj_up_avg + proj_down_avg)
    }

    pub fn avg_cost(&self, market_id: &str, side: MarketSide) -> Option<Decimal> {
        self.cost_basis
            .get(&(market_id.to_string(), side))
            .and_then(|basis| {
                if basis.qty > Decimal::ZERO {
                    Some(basis.cost / basis.qty)
                } else {
                    None
                }
            })
    }

    pub fn pnl_snapshot(
        &mut self,
        market_id: &str,
        up_mid: Decimal,
        down_mid: Decimal,
    ) -> PnlSnapshot {
        let cash = self.market_cash.get(market_id).cloned().unwrap_or(Decimal::ZERO);
        let position = self.positions.get(market_id).cloned().unwrap_or_default();
        let inventory_value = (position.up_shares * up_mid) + (position.down_shares * down_mid);
        let total = cash + inventory_value;
        let base = self.pnl_base.entry(market_id.to_string()).or_insert(total);
        PnlSnapshot {
            cash,
            inventory_value,
            total: total - *base,
        }
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

        if up_shares > Decimal::ZERO {
            self.buy_fills
                .insert((market_id.clone(), MarketSide::Up), true);
            self.cost_basis
                .entry((market_id.clone(), MarketSide::Up))
                .or_insert(CostBasis::default())
                .qty = up_shares;
        }
        if down_shares > Decimal::ZERO {
            self.buy_fills
                .insert((market_id.clone(), MarketSide::Down), true);
            self.cost_basis
                .entry((market_id.clone(), MarketSide::Down))
                .or_insert(CostBasis::default())
                .qty = down_shares;
        }

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
            if matches!(msg.msg_type, Some(OrderMessageType::Placement)) {
                let token_id = msg.asset_id.to_string();
                if let Some((market_id, side)) = self.token_to_market.get(&token_id).cloned() {
                    if let Some(original_size) = msg.original_size {
                        let order_side = match msg.side {
                            polymarket_client_sdk::clob::types::Side::Buy => OrderSide::Buy,
                            polymarket_client_sdk::clob::types::Side::Sell => OrderSide::Sell,
                            _ => OrderSide::Buy,
                        };
                        self.tracked_orders.insert(
                            order_id.clone(),
                            TrackedOrder {
                                order_id: order_id.clone(),
                                market_id: market_id.clone(),
                                side,
                                order_side,
                                original_size,
                                filled_size: Decimal::ZERO,
                                price: msg.price,
                                is_open: true,
                                placed_at: Instant::now(),
                                first_update_logged: false,
                            },
                        );
                        debug!(
                            order_id = %short_id(&order_id, 8),
                            market = %self.market_label(&market_id),
                            side = ?side,
                            order_side = ?order_side,
                            size = %original_size,
                            price = %msg.price,
                            "Order tracked from WS placement"
                        );
                        return;
                    }
                }
            }
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

        let (fill_info, close_info) = {
            let mut fill_info = None;
            let mut close_info = None;
            if let Some(size_matched) = msg.size_matched {
                let fill_delta = size_matched - tracked.filled_size;
                if fill_delta > Decimal::ZERO {
                    let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                    match tracked.side {
                        MarketSide::Up => {
                            if tracked.order_side == OrderSide::Buy {
                                pos.up_shares += fill_delta;
                            } else {
                                pos.up_shares -= fill_delta;
                            }
                        }
                        MarketSide::Down => {
                            if tracked.order_side == OrderSide::Buy {
                                pos.down_shares += fill_delta;
                            } else {
                                pos.down_shares -= fill_delta;
                            }
                        }
                    }

                    tracked.filled_size = size_matched;
                    fill_info = Some((
                        tracked.market_id.clone(),
                        tracked.side,
                        tracked.order_side,
                        tracked.price,
                        fill_delta,
                        size_matched,
                    ));
                }
            }

            let is_cancelled = matches!(msg.msg_type, Some(OrderMessageType::Cancellation));
            let is_fully_matched = matches!(
                (msg.original_size, msg.size_matched),
                (Some(original), Some(matched)) if matched >= original
            );
            let is_closed = is_cancelled || is_fully_matched;

            if is_closed && tracked.is_open {
                tracked.is_open = false;
                close_info = Some(msg.msg_type);
            }

            (fill_info, close_info)
        };

        if let Some((market_id, side, order_side, price, fill_delta, total_filled)) = fill_info {
            self.record_cash_and_fills(market_id.clone(), side, order_side, price, fill_delta);
            info!(
                order_id = %short_id(&order_id, 8),
                side = ?side,
                order_side = ?order_side,
                fill_delta = %fill_delta,
                total_filled = %total_filled,
                market = %market_id,
                "Fill detected via WebSocket"
            );
        }

        if let Some(msg_type) = close_info {
            debug!(
                order_id = %short_id(&order_id, 8),
                msg_type = ?msg_type,
                "Order closed via WebSocket"
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
                order_side: o.order_side,
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

        let fill_info = {
            let mut fill_info = None;
            let fill_delta = filled_size - tracked.filled_size;
            if fill_delta > Decimal::ZERO {
                let pos = self.positions.entry(tracked.market_id.clone()).or_default();
                match tracked.side {
                    MarketSide::Up => {
                        if tracked.order_side == OrderSide::Buy {
                            pos.up_shares += fill_delta;
                        } else {
                            pos.up_shares -= fill_delta;
                        }
                    }
                    MarketSide::Down => {
                        if tracked.order_side == OrderSide::Buy {
                            pos.down_shares += fill_delta;
                        } else {
                            pos.down_shares -= fill_delta;
                        }
                    }
                }

                tracked.filled_size = filled_size;
                fill_info = Some((
                    tracked.market_id.clone(),
                    tracked.side,
                    tracked.order_side,
                    tracked.price,
                    fill_delta,
                    filled_size,
                ));
            }

            tracked.is_open = is_open;
            fill_info
        };

        if let Some((market_id, side, order_side, price, fill_delta, total_filled)) = fill_info {
            self.record_cash_and_fills(market_id, side, order_side, price, fill_delta);
            debug!(
                order_id = %short_id(order_id, 8),
                side = ?side,
                order_side = ?order_side,
                fill_delta = %fill_delta,
                total_filled = %total_filled,
                "Reconciled fill via order status"
            );
        }
    }

    pub fn confirmed_position(&self, market_id: &str) -> (Decimal, Decimal) {
        self.positions
            .get(market_id)
            .map(|p| (p.up_shares, p.down_shares))
            .unwrap_or_default()
    }

    fn record_cash_and_fills(
        &mut self,
        market_id: String,
        side: MarketSide,
        order_side: OrderSide,
        price: Decimal,
        fill_delta: Decimal,
    ) {
        let cash = self
            .market_cash
            .entry(market_id.clone())
            .or_insert(Decimal::ZERO);
        let notional = price * fill_delta;
        match order_side {
            OrderSide::Buy => {
                *cash -= notional;
                self.buy_fills.insert((market_id.clone(), side), true);
            }
            OrderSide::Sell => {
                *cash += notional;
            }
        }

        let basis = self
            .cost_basis
            .entry((market_id, side))
            .or_insert(CostBasis::default());
        match order_side {
            OrderSide::Buy => {
                basis.qty += fill_delta;
                basis.cost += notional;
            }
            OrderSide::Sell => {
                if basis.qty > Decimal::ZERO {
                    let avg_cost = if basis.cost > Decimal::ZERO {
                        basis.cost / basis.qty
                    } else {
                        Decimal::ZERO
                    };
                    let reduction = avg_cost * fill_delta;
                    basis.cost = (basis.cost - reduction).max(Decimal::ZERO);
                    basis.qty = (basis.qty - fill_delta).max(Decimal::ZERO);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PnlSnapshot {
    pub cash: Decimal,
    pub inventory_value: Decimal,
    pub total: Decimal,
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
