// ladder.rs
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::info;

use crate::{
    constants::{round_size, short_id},
    poller::{MarketPosition, MarketSide},
};
const MIN_ORDER_SIZE: Decimal = dec!(5);

pub struct LadderConfig {
    pub levels: usize,
    pub spacing: Decimal,
    pub size_per_level: Decimal,
    pub top_offset: Decimal,
    pub shares_per_round: Decimal,
    pub reladder_threshold: Decimal,
    pub stale_order_distance: Decimal,
    
    // Round-based safety settings
    pub max_rounds_per_market: usize,
    pub max_unpaired_exposure: Decimal,
    pub min_seconds_before_end: i64,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            levels: 2,                          // Only 2 levels - minimal
            spacing: dec!(0.01),                // 1¢ between levels
            size_per_level: dec!(10),           // Larger size per level
            top_offset: dec!(0.005),            // Half-cent below ask - very close!
            shares_per_round: dec!(20),         // 20 shares per side per round
            reladder_threshold: dec!(0.05),     // Only reladder if price moves 5¢ - STICKY
            stale_order_distance: dec!(0.15),   // Only cancel if 15¢ away - very tolerant
            
            // Round-based safety
            max_rounds_per_market: 5,           // Max 5 complete rounds per market
            max_unpaired_exposure: dec!(25),    // Never have more than 25 unpaired shares
            min_seconds_before_end: 180,        // Stop 3 minutes before market ends
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
        pending_up: Decimal,
        pending_down: Decimal,
        open_orders: &[OpenOrderInfo],
        locked_pairs: Decimal,  // Pairs already completed in previous rounds
        max_up_price: Option<Decimal>,   // Max price for Up to stay profitable (from pair tracker)
        max_down_price: Option<Decimal>, // Max price for Down to stay profitable
    ) -> LadderPlan {
        let mut plan = LadderPlan::default();

        // Subtract locked pairs from position - those are "done"
        let working_up = (position.up_shares - locked_pairs).max(Decimal::ZERO);
        let working_down = (position.down_shares - locked_pairs).max(Decimal::ZERO);

        let target = self.config.shares_per_round;
        let min_side = working_up.min(working_down);
        let desired_per_side = if min_side >= target {
            working_up.max(working_down)
        } else {
            target
        };

        /* for both sides, if we are over, then cancel all pending orders. */
        if working_up >= desired_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Up) {
                plan.cancellations.push(order.order_id.clone());
            }
        }

        if working_down >= desired_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Down) {
                plan.cancellations.push(order.order_id.clone());
            }
        }

        /* only cancel actually stale orders */
        for order_id in self.find_stale_orders(up_ask, down_ask, open_orders) {
            if !plan.cancellations.contains(&order_id) {
                plan.cancellations.push(order_id);
            }
        }

        /* find the price levels that are already covered by open orders. */
        let covered_up: HashSet<Decimal> = open_orders
            .iter()
            .filter(|o| o.side == MarketSide::Up && !plan.cancellations.contains(&o.order_id))
            .map(|o| o.price)
            .collect();

        let covered_down: HashSet<Decimal> = open_orders
            .iter()
            .filter(|o| o.side == MarketSide::Down && !plan.cancellations.contains(&o.order_id))
            .map(|o| o.price)
            .collect();

        /* calculate the pending room we have after cancellations */
        let cancelled_up: Decimal = open_orders
            .iter()
            .filter(|o| o.side == MarketSide::Up && plan.cancellations.contains(&o.order_id))
            .map(|o| o.remaining_size)
            .sum();

        let cancelled_down: Decimal = open_orders
            .iter()
            .filter(|o| o.side == MarketSide::Down && plan.cancellations.contains(&o.order_id))
            .map(|o| o.remaining_size)
            .sum();

        let effective_pending_up = (pending_up - cancelled_up).max(Decimal::ZERO);
        let effective_pending_down = (pending_down - cancelled_down).max(Decimal::ZERO);

        // 5. Calculate room for new orders (based on working position, not total)
        let up_room = if working_up >= desired_per_side {
            Decimal::ZERO
        } else {
            (desired_per_side - working_up - effective_pending_up).max(Decimal::ZERO)
        };

        let down_room = if working_down >= desired_per_side {
            Decimal::ZERO
        } else {
            (desired_per_side - working_down - effective_pending_down).max(Decimal::ZERO)
        };

        // 6. Generate ladders, skipping already-covered price levels
        // Also respect max hedge prices to avoid chasing unprofitable fills
        if up_room >= MIN_ORDER_SIZE {
            for order in self.generate_side_ladder(MarketSide::Up, up_ask, up_room, max_up_price) {
                if !covered_up.contains(&order.price) {
                    plan.orders.push(order);
                }
            }
        }

        if down_room >= MIN_ORDER_SIZE {
            for order in self.generate_side_ladder(MarketSide::Down, down_ask, down_room, max_down_price) {
                if !covered_down.contains(&order.price) {
                    plan.orders.push(order);
                }
            }
        }

        plan
    }
    fn find_stale_orders(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        open_orders: &[OpenOrderInfo],
    ) -> Vec<String> {
        let mut to_cancel = Vec::new();

        for order in open_orders {
            let current_ask = match order.side {
                MarketSide::Up => up_ask,
                MarketSide::Down => down_ask,
            };

            /* If a pending order is too far below the current ask, it's stale. */
            let distance = current_ask - order.price;
            if distance > self.config.stale_order_distance {
                let short_id = short_id(&order.order_id, 8);
                info!(
                    order_id = short_id,
                    price = %order.price,
                    current_ask = %current_ask,
                    distance = %distance,
                    "Order too far from market, marking for cancellation"
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
        max_price: Option<Decimal>,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();

        if room <= Decimal::ZERO {
            return orders;
        }

        // If we have unpaired shares on the opposite side, we can only buy at max_price
        // to stay profitable. If market has moved past this, skip placing orders.
        let effective_max = max_price.unwrap_or(dec!(0.99));
        let top_price = (ask - self.config.top_offset).max(dec!(0.01));
        
        // If our desired price is above the max profitable price, don't place orders
        if top_price > effective_max {
            info!(
                side = ?side,
                ask = %ask,
                top_price = %top_price,
                max_price = %effective_max,
                "Price too high to be profitable, skipping orders"
            );
            return orders;
        }

        let mut remaining_room = room;

        for level in 0..self.config.levels {
            let price =
                (top_price - self.config.spacing * Decimal::from(level as u32)).max(dec!(0.01));

            if price >= ask {
                continue;
            }
            
            // Don't place orders above our max profitable price
            if price > effective_max {
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

/* The structure of an open order. Used to help categorize stale orders. */
#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub order_id: String,
    pub side: MarketSide,
    pub price: Decimal,
    pub remaining_size: Decimal,
}
