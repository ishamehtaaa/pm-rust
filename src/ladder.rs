/* ladder.rs - Order ladder generation and management */
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, trace};

/* Patience multiplier for momentum-aware pricing */
const PATIENCE_MULT: Decimal = dec!(2.0);
/* Chase multiplier for elusive side (place tighter to ask) */
const CHASE_MULT: Decimal = dec!(0.5);

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
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            levels: 3,
            spacing: dec!(0.01),
            size_per_level: dec!(5),
            top_offset: dec!(0.02),
            max_pair_cost: dec!(0.97),
            tick_size: dec!(0.01),
            target_per_side: dec!(10),
            max_position_per_side: dec!(50),
            max_pending_per_side: dec!(20),
            reladder_threshold: dec!(0.02),
            stale_order_distance: dec!(0.10),  /* Relaxed: keep orders longer in wide-spread markets */
            max_imbalance: dec!(5), // Don't let one side get more than 10 shares ahead
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

/* Tracks the last price we laddered at for each market */
#[derive(Debug, Default)]
pub struct LadderState {
    /* market_id -> (last_up_ask, last_down_ask) */
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

        /* Calculate pending from open orders (source of truth from remote state) */
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

        /* Cancel ALL orders on sides that are already at or over target */
        if position.up_shares >= target_per_side {
            for order in open_orders.iter().filter(|o| o.side == MarketSide::Up) {
                info!(
                    order_id = %order.order_id,
                    up_shares = %position.up_shares,
                    target = %target_per_side,
                    "Cancelling UP order - already at target"
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
                    "Cancelling DOWN order - already at target"
                );
                plan.cancellations.push(order.order_id.clone());
            }
        }

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

        /* Calculate room for new orders (only if position is below target) */
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

        /* Generate paired ladders only when both sides have room */
        if up_room >= MIN_ORDER_SIZE && down_room >= MIN_ORDER_SIZE {
            let pair_orders =
                self.generate_paired_ladder(up_ask, down_ask, up_room, down_room, overrides);
            plan.orders.extend(pair_orders);
        } else if let Some(side) = overrides.allow_imbalance_side {
            let single_orders =
                self.generate_single_ladder(side, up_ask, down_ask, up_room, down_room, overrides);
            plan.orders.extend(single_orders);
        }

        plan
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

            /* Cancel if too far from market */
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

            /* Cancel orders on the heavy side to rebalance */
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

    fn generate_paired_ladder(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        up_room: Decimal,
        down_room: Decimal,
        overrides: LadderOverrides,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();

        /* If either price cap is None, we have no edge on that side.
         * For paired orders, we need edge on BOTH sides. */
        let up_cap = match overrides.up_price_cap {
            Some(cap) => cap,
            None => {
                trace!("No edge on Up side - skipping paired ladder");
                return orders;
            }
        };
        let down_cap = match overrides.down_price_cap {
            Some(cap) => cap,
            None => {
                trace!("No edge on Down side - skipping paired ladder");
                return orders;
            }
        };

        let mut remaining_room = up_room.min(down_room);
        if remaining_room <= Decimal::ZERO {
            return orders;
        }

        let size_per_level =
            (self.config.size_per_level * overrides.size_multiplier).max(MIN_ORDER_SIZE);
        let spacing = self.config.spacing * overrides.spacing_multiplier;
        let base_offset = self.config.top_offset + overrides.extra_offset;
        
        /*
         * Momentum-aware asymmetric pricing:
         * - When Up trending up (momentum > 0): bid tight on Up (elusive), wide on Down (cheap)
         * - When Up trending down (momentum < 0): bid tight on Down (elusive), wide on Up (cheap)
         * - The "elusive" side is chased aggressively, the "cheap" side we wait for
         */
        const MOMENTUM_THRESHOLD: f64 = 0.15;
        
        let (up_offset, down_offset) = if overrides.momentum > MOMENTUM_THRESHOLD {
            /* Up is elusive (trending up), Down is cheap (will get cheaper) */
            (base_offset * CHASE_MULT, base_offset * PATIENCE_MULT)
        } else if overrides.momentum < -MOMENTUM_THRESHOLD {
            /* Down is elusive (trending up), Up is cheap */
            (base_offset * PATIENCE_MULT, base_offset * CHASE_MULT)
        } else {
            /* Stable market - bid normally on both */
            (base_offset, base_offset)
        };
        
        /* Apply price caps - the MAXIMUM we're willing to pay */
        let top_up = (up_ask - up_offset).max(dec!(0.01)).min(up_cap);
        let top_down = (down_ask - down_offset).max(dec!(0.01)).min(down_cap);
        
        let tick = self.config.tick_size.max(dec!(0.01));
        let levels = overrides.max_levels.unwrap_or(self.config.levels);

        for level in 0..levels {
            let price_up = (top_up - spacing * Decimal::from(level as u32)).max(dec!(0.01));
            let price_down = (top_down - spacing * Decimal::from(level as u32)).max(dec!(0.01));

            let price_up = floor_to_tick(price_up, tick);
            let price_down = floor_to_tick(price_down, tick);

            if price_up >= up_ask || price_down >= down_ask {
                continue;
            }

            /* Combined cost must be below threshold */
            if price_up + price_down > self.config.max_pair_cost {
                debug!(
                    up = %price_up,
                    down = %price_down,
                    combined = %(price_up + price_down),
                    max = %self.config.max_pair_cost,
                    "Combined cost too high - skipping level"
                );
                continue;
            }

            let size = round_size(size_per_level.min(remaining_room));
            if size < MIN_ORDER_SIZE {
                break;
            }

            /* 
             * MOMENTUM-AWARE ORDER PRIORITY
             * Buy the "elusive" side first (the one getting more expensive).
             * - momentum > 0: Up is rising → buy Up first
             * - momentum < 0: Down is rising → buy Down first
             * This ensures we catch the side that's moving away from us.
             */
            if overrides.momentum < -0.1 {
                /* Down is trending up (getting expensive) → buy Down first */
                orders.push(LadderOrder {
                    side: MarketSide::Down,
                    price: price_down,
                    size,
                });
                orders.push(LadderOrder {
                    side: MarketSide::Up,
                    price: price_up,
                    size,
                });
            } else {
                /* Up is trending up or stable → buy Up first (default) */
                orders.push(LadderOrder {
                    side: MarketSide::Up,
                    price: price_up,
                    size,
                });
                orders.push(LadderOrder {
                    side: MarketSide::Down,
                    price: price_down,
                    size,
                });
            }
            remaining_room -= size;
        }
        orders
    }

    fn generate_single_ladder(
        &self,
        side: MarketSide,
        up_ask: Decimal,
        down_ask: Decimal,
        up_room: Decimal,
        down_room: Decimal,
        overrides: LadderOverrides,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();
        
        /* Check if we have edge on this side (price cap exists) */
        let price_cap = match side {
            MarketSide::Up => overrides.up_price_cap,
            MarketSide::Down => overrides.down_price_cap,
        };
        
        let cap = match price_cap {
            Some(cap) => cap,
            None => {
                trace!(side = ?side, "No edge on side - skipping single ladder");
                return orders;
            }
        };
        
        let room = match side {
            MarketSide::Up => up_room,
            MarketSide::Down => down_room,
        };

        if room <= Decimal::ZERO {
            return orders;
        }

        let size_per_level =
            (self.config.size_per_level * overrides.size_multiplier).max(MIN_ORDER_SIZE);
        let spacing = self.config.spacing * overrides.spacing_multiplier;
        let top_offset = self.config.top_offset + overrides.extra_offset;
        let tick = self.config.tick_size.max(dec!(0.01));
        let levels = overrides.max_levels.unwrap_or(self.config.levels);

        let top_price = match side {
            MarketSide::Up => (up_ask - top_offset).max(dec!(0.01)).min(cap),
            MarketSide::Down => (down_ask - top_offset).max(dec!(0.01)).min(cap),
        };

        let mut remaining_room = room;
        for level in 0..levels {
            let price = (top_price - spacing * Decimal::from(level as u32)).max(dec!(0.01));
            let price = floor_to_tick(price, tick);

            let current_ask = match side {
                MarketSide::Up => up_ask,
                MarketSide::Down => down_ask,
            };

            if price >= current_ask {
                continue;
            }

            let size = round_size(size_per_level.min(remaining_room));
            if size < MIN_ORDER_SIZE {
                break;
            }

            orders.push(LadderOrder { side, price, size });
            remaining_room -= size;
        }

        orders
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
    /* Momentum: positive = Up trending up, negative = Up trending down */
    pub momentum: f64,
}

impl Default for LadderOverrides {
    fn default() -> Self {
        Self {
            size_multiplier: Decimal::ONE,
            spacing_multiplier: Decimal::ONE,
            extra_offset: Decimal::ZERO,
            up_price_cap: None,
            down_price_cap: None,
            momentum: 0.0,
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

/* Info about an open order, used for stale detection */
#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub order_id: String,
    pub side: MarketSide,
    pub price: Decimal,
    pub remaining_size: Decimal,
}
