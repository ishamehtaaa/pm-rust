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
    pub max_pair_cost: Decimal,
    pub tick_size: Decimal,
    pub target_per_side: Decimal,
    pub max_position_per_side: Decimal,
    pub max_pending_per_side: Decimal,
    pub reladder_threshold: Decimal,
    pub stale_order_distance: Decimal,
    pub max_imbalance: Decimal,
    // Continuous trading mode
    pub allow_single_side: bool,
    pub aggressive_rebalance_threshold: Decimal,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            levels: 3,
            spacing: dec!(0.01),
            size_per_level: dec!(24),
            top_offset: dec!(0.01),
            max_pair_cost: dec!(0.995),
            tick_size: dec!(0.01),
            target_per_side: dec!(5000),
            max_position_per_side: dec!(5000),
            max_pending_per_side: dec!(200),
            reladder_threshold: dec!(0.02),
            stale_order_distance: dec!(0.10),
            max_imbalance: dec!(200),
            allow_single_side: true,
            aggressive_rebalance_threshold: dec!(500),
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
        self.compute_ladder_with_target(up_ask, down_ask, position, open_orders, None, None)
    }

    pub fn compute_ladder_with_target(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        position: &MarketPosition,
        open_orders: &[OpenOrderInfo],
        target_per_side: Option<Decimal>,
        overrides: Option<LadderOverrides>,
    ) -> LadderPlan {
        let mut plan = LadderPlan::default();
        let target_per_side = target_per_side.unwrap_or(self.config.target_per_side);
        let overrides = overrides.unwrap_or_default();

        // Calculate actual pending from open orders
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
        let imbalance = total_up - total_down;

        // Cancel orders on sides that are at or over target
        if position.up_shares >= target_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Up) {
                info!(
                    order_id = %order.order_id,
                    up_shares = %position.up_shares,
                    target = %target_per_side,
                    "Cancelling UP order - at target"
                );
                plan.cancellations.push(order.order_id.clone());
            }
        }

        if position.down_shares >= target_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Down) {
                info!(
                    order_id = %order.order_id,
                    down_shares = %position.down_shares,
                    target = %target_per_side,
                    "Cancelling DOWN order - at target"
                );
                plan.cancellations.push(order.order_id.clone());
            }
        }

        // Find stale orders
        let stale = self.find_stale_orders(
            up_ask,
            down_ask,
            open_orders,
            imbalance,
            overrides.allow_imbalance_side,
        );
        for order_id in stale {
            if !plan.cancellations.contains(&order_id) {
                plan.cancellations.push(order_id);
            }
        }

        // Calculate room for new orders
        let up_room = if position.up_shares >= target_per_side {
            Decimal::ZERO
        } else {
            (target_per_side - total_up).max(Decimal::ZERO)
        };

        let down_room = if position.down_shares >= target_per_side {
            Decimal::ZERO
        } else {
            (target_per_side - total_down).max(Decimal::ZERO)
        };

        if up_room.is_zero() && down_room.is_zero() {
            trace!("No room for new orders");
            return plan;
        }

        // === CONTINUOUS TRADING MODE ===
        // Key insight: place orders on both sides independently, prioritizing the lighter side
        
        // Determine priority based on imbalance
        let (priority_side, secondary_side) = if imbalance > self.config.max_imbalance {
            (MarketSide::Down, MarketSide::Up)
        } else if imbalance < -self.config.max_imbalance {
            (MarketSide::Up, MarketSide::Down)
        } else {
            // Balanced - use cheaper side as priority
            if up_ask <= down_ask {
                (MarketSide::Up, MarketSide::Down)
            } else {
                (MarketSide::Down, MarketSide::Up)
            }
        };

        // Generate orders for priority side first
        let priority_room = match priority_side {
            MarketSide::Up => up_room,
            MarketSide::Down => down_room,
        };
        let priority_ask = match priority_side {
            MarketSide::Up => up_ask,
            MarketSide::Down => down_ask,
        };

        if priority_room >= MIN_ORDER_SIZE {
            let orders = self.generate_continuous_ladder(
                priority_side,
                priority_ask,
                priority_room,
                overrides,
            );
            plan.orders.extend(orders);
        }

        // Generate orders for secondary side (if allow_single_side OR both have room)
        let secondary_room = match secondary_side {
            MarketSide::Up => up_room,
            MarketSide::Down => down_room,
        };
        let secondary_ask = match secondary_side {
            MarketSide::Up => up_ask,
            MarketSide::Down => down_ask,
        };

        let should_place_secondary = secondary_room >= MIN_ORDER_SIZE
            && (self.config.allow_single_side || priority_room >= MIN_ORDER_SIZE);

        if should_place_secondary {
            let orders = self.generate_continuous_ladder(
                secondary_side,
                secondary_ask,
                secondary_room,
                overrides,
            );
            plan.orders.extend(orders);
        }

        plan
    }

    /// Generate a simple ladder for continuous trading.
    /// Places orders at competitive prices to get filled.
    fn generate_continuous_ladder(
        &self,
        side: MarketSide,
        ask: Decimal,
        room: Decimal,
        overrides: LadderOverrides,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();
        let mut remaining_room = room;

        let size_per_level = (self.config.size_per_level * overrides.size_multiplier).max(MIN_ORDER_SIZE);
        let spacing = self.config.spacing * overrides.spacing_multiplier;
        let top_offset = self.config.top_offset + overrides.extra_offset;
        let tick = self.config.tick_size.max(dec!(0.01));
        let levels = overrides.max_levels.unwrap_or(self.config.levels);

        // Check price cap if exists
        let price_cap = match side {
            MarketSide::Up => overrides.up_price_cap,
            MarketSide::Down => overrides.down_price_cap,
        };

        // Top price: just below the ask
        let mut top_price = (ask - top_offset).max(dec!(0.01));
        if let Some(cap) = price_cap {
            top_price = top_price.min(cap);
        }

        for level in 0..levels {
            let price = (top_price - spacing * Decimal::from(level as u32)).max(dec!(0.01));
            let price = floor_to_tick(price, tick);

            // Don't place orders at or above the ask
            if price >= ask {
                continue;
            }

            let size = round_size(size_per_level.min(remaining_room));
            if size < MIN_ORDER_SIZE {
                break;
            }

            orders.push(LadderOrder { side, price, size });
            remaining_room -= size;

            if remaining_room < MIN_ORDER_SIZE {
                break;
            }
        }

        orders
    }

    fn find_stale_orders(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        open_orders: &[OpenOrderInfo],
        imbalance: Decimal, // FIXED: This is now the actual imbalance value
        allow_imbalance_side: Option<MarketSide>,
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
                debug!(
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

            if should_cancel_for_balance && allow_imbalance_side != Some(order.side) {
                debug!(
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

}

#[derive(Debug, Clone, Copy)]
pub struct LadderOverrides {
    pub size_multiplier: Decimal,
    pub spacing_multiplier: Decimal,
    pub extra_offset: Decimal,
    pub up_price_cap: Option<Decimal>,
    pub down_price_cap: Option<Decimal>,
    pub max_levels: Option<usize>,
    pub allow_imbalance_side: Option<MarketSide>,
}

impl Default for LadderOverrides {
    fn default() -> Self {
        Self {
            size_multiplier: Decimal::ONE,
            spacing_multiplier: Decimal::ONE,
            extra_offset: Decimal::ZERO,
            up_price_cap: None,
            down_price_cap: None,
            max_levels: None,
            allow_imbalance_side: None,
        }
    }
}

fn floor_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    let ticks = (price / tick).floor();
    (ticks * tick).max(dec!(0.01))
}

/// Info about an open order, used for stale detection
#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub order_id: String,
    pub side: MarketSide,
    pub price: Decimal,
    pub remaining_size: Decimal,
}
