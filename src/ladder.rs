// ladder.rs - FIXED VERSION
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, trace};

use crate::{
    constants::round_size,
    poller::{MarketPosition, MarketSide},
};
const MIN_ORDER_SIZE: Decimal = dec!(5);

pub struct LadderConfig {
    pub levels: usize,
    pub spacing: Decimal,
    pub size_per_level: Decimal,
    pub top_offset: Decimal,
    pub target_per_side: Decimal,
    pub max_position_per_side: Decimal,
    pub max_pending_per_side: Decimal,
    pub reladder_threshold: Decimal,
    pub stale_order_distance: Decimal,
    pub max_imbalance: Decimal,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            levels: 3,
            spacing: dec!(0.01),
            size_per_level: dec!(5),
            top_offset: dec!(0.02),
            target_per_side: dec!(10),
            max_position_per_side: dec!(50),
            max_pending_per_side: dec!(20),
            reladder_threshold: dec!(0.02),
            stale_order_distance: dec!(0.05),
            max_imbalance: dec!(10), // Don't let one side get more than 10 shares ahead
        }
    }
}

#[derive(Debug, Clone)]
pub struct LadderOrder {
    pub side: MarketSide,
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Default)]
pub struct LadderPlan {
    pub orders: Vec<LadderOrder>,
    pub cancellations: Vec<String>, // Order IDs to cancel
}

/// Tracks the last price we laddered at for each market
#[derive(Debug, Default)]
pub struct LadderState {
    /// market_id -> (last_up_ask, last_down_ask)
    last_ladder_prices: HashMap<String, (Decimal, Decimal)>,
}

impl LadderState {
    pub fn should_reladder(
        &self,
        market_id: &str,
        up_ask: Decimal,
        down_ask: Decimal,
        threshold: Decimal,
    ) -> bool {
        match self.last_ladder_prices.get(market_id) {
            Some((last_up, last_down)) => {
                let up_diff = (up_ask - *last_up).abs();
                let down_diff = (down_ask - *last_down).abs();
                up_diff >= threshold || down_diff >= threshold
            }
            None => true, // Never laddered, so yes
        }
    }

    pub fn record_ladder(&mut self, market_id: String, up_ask: Decimal, down_ask: Decimal) {
        self.last_ladder_prices
            .insert(market_id, (up_ask, down_ask));
    }

    pub fn clear_market(&mut self, market_id: &str) {
        self.last_ladder_prices.remove(market_id);
    }
}

pub struct LadderEngine {
    config: LadderConfig,
}

impl LadderEngine {
    pub fn new(config: LadderConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &LadderConfig {
        &self.config
    }

    /* Compute cancellations + orders to place based on current state. */
    pub fn compute_ladder(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        position: &MarketPosition,
        open_orders: &[OpenOrderInfo],
    ) -> LadderPlan {
        let mut plan = LadderPlan::default();

        // Calculate actual pending from open orders (not from position.pending_*)
        // This is the source of truth since it's based on actual remote order state
        let pending_up: Decimal = open_orders
            .iter()
            .filter(|o| o.side == MarketSide::Up)
            .map(|o| o.remaining_size)
            .sum();

        let pending_down: Decimal = open_orders
            .iter()
            .filter(|o| o.side == MarketSide::Down)
            .map(|o| o.remaining_size)
            .sum();

        let total_up = position.up_shares + pending_up;
        let total_down = position.down_shares + pending_down;
        
        // CRITICAL FIX: Calculate imbalance here
        let imbalance = total_up - total_down;

        debug!(
            up_shares = %position.up_shares,
            down_shares = %position.down_shares,
            pending_up = %pending_up,
            pending_down = %pending_down,
            total_up = %total_up,
            total_down = %total_down,
            imbalance = %imbalance,
            target = %self.config.target_per_side,
            "Position check"
        );

        // Cancel ALL orders on sides that are already at or over target
        if position.up_shares >= self.config.target_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Up) {
                info!(
                    order_id = %order.order_id,
                    up_shares = %position.up_shares,
                    target = %self.config.target_per_side,
                    "Cancelling UP order - already at target"
                );
                plan.cancellations.push(order.order_id.clone());
            }
        }

        if position.down_shares >= self.config.target_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Down) {
                info!(
                    order_id = %order.order_id,
                    down_shares = %position.down_shares,
                    target = %self.config.target_per_side,
                    "Cancelling DOWN order - already at target"
                );
                plan.cancellations.push(order.order_id.clone());
            }
        }

        // CRITICAL FIX: Pass the calculated imbalance, not config.max_imbalance
        let stale = self.find_stale_orders(up_ask, down_ask, open_orders, imbalance);
        for order_id in stale {
            if !plan.cancellations.contains(&order_id) {
                plan.cancellations.push(order_id);
            }
        }

        // Calculate room for new orders (only if position is below target)
        let up_room = if position.up_shares >= self.config.target_per_side {
            Decimal::ZERO
        } else {
            (self.config.target_per_side - total_up).max(Decimal::ZERO)
        };

        let down_room = if position.down_shares >= self.config.target_per_side {
            Decimal::ZERO
        } else {
            (self.config.target_per_side - total_down).max(Decimal::ZERO)
        };

        if up_room.is_zero() && down_room.is_zero() {
            trace!("No room for new orders");
            return plan;
        }

        // Generate ladders only for sides with room
        if up_room >= MIN_ORDER_SIZE {
            let up_orders = self.generate_side_ladder(MarketSide::Up, up_ask, up_room);
            plan.orders.extend(up_orders);
        }

        if down_room >= MIN_ORDER_SIZE {
            let down_orders = self.generate_side_ladder(MarketSide::Down, down_ask, down_room);
            plan.orders.extend(down_orders);
        }

        plan
    }

    fn find_stale_orders(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        open_orders: &[OpenOrderInfo],
        imbalance: Decimal,  // FIXED: This is now the actual imbalance value
    ) -> Vec<String> {
        let mut to_cancel = Vec::new();

        for order in open_orders {
            let current_ask = match order.side {
                MarketSide::Up => up_ask,
                MarketSide::Down => down_ask,
            };

            // Cancel if too far from market
            let distance = current_ask - order.price;
            if distance > self.config.stale_order_distance {
                info!(
                    order_id = %order.order_id,
                    price = %order.price,
                    current_ask = %current_ask,
                    distance = %distance,
                    "Order too far from market"
                );
                to_cancel.push(order.order_id.clone());
                continue;
            }

            // Cancel orders on the heavy side to rebalance
            let should_cancel_for_balance = match order.side {
                MarketSide::Up => imbalance > self.config.max_imbalance,
                MarketSide::Down => imbalance < -self.config.max_imbalance,
            };

            if should_cancel_for_balance {
                info!(
                    order_id = %order.order_id,
                    side = ?order.side,
                    imbalance = %imbalance,
                    max_imbalance = %self.config.max_imbalance,
                    "Cancelling to rebalance"
                );
                to_cancel.push(order.order_id.clone());
            }
        }

        to_cancel
    }

    fn generate_side_ladder(
        &self,
        side: MarketSide,
        ask: Decimal,
        room: Decimal,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();

        if room <= Decimal::ZERO {
            return orders;
        }

        let mut remaining_room = room;
        let top_price = (ask - self.config.top_offset).max(dec!(0.01));

        for level in 0..self.config.levels {
            let price =
                (top_price - self.config.spacing * Decimal::from(level as u32)).max(dec!(0.01));

            if price >= ask {
                continue;
            }

            let size = round_size(self.config.size_per_level.min(remaining_room));
            if size < MIN_ORDER_SIZE {
                break;
            }

            orders.push(LadderOrder { side, price, size });
            remaining_room -= size;
        }
        orders
    }
}

/// Info about an open order, used for stale detection
#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub order_id: String,
    pub side: MarketSide,
    pub price: Decimal,
    pub remaining_size: Decimal,
}