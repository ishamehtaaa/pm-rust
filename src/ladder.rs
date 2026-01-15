/* ladder.rs - Order ladder generation and management */
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, trace};

/* Patience multiplier for momentum-aware pricing */
const PATIENCE_MULT: Decimal = dec!(2.0);

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
    pub max_hedge_loss: Decimal,  /* Allow this much loss to complete a hedge (e.g. 0.02 = 2¢) */
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
            top_offset: dec!(0.01),  /* Reduced from 2¢ to 1¢ - conviction scales this further */
            max_pair_cost: dec!(0.97),
            max_hedge_loss: dec!(0.00),  /* No loss tolerance - only break-even or profitable hedges */
            tick_size: dec!(0.01),
            target_per_side: dec!(10),
            max_position_per_side: dec!(50),
            max_pending_per_side: dec!(20),
            reladder_threshold: dec!(0.01),  /* React to 1¢ moves instead of 2¢ */
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
            &overrides,
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
        
        /* 
         * REBALANCING ROOM: When rebalancing, we may need to buy MORE of the
         * light side to match the heavy side, even if we're at target.
         * 
         * Example: 45 Up, 40 Down, target=40
         * - up_room = 0 (over target)
         * - down_room = 0 (at target)
         * - But we need to buy 5 more Down to equalize!
         */
        let rebalance_room = imbalance.abs();
        let (up_room, down_room) = match overrides.allow_imbalance_side {
            Some(MarketSide::Up) if imbalance < Decimal::ZERO => {
                /* Need more Up to match Down - allow extra room */
                (up_room.max(rebalance_room), down_room)
            }
            Some(MarketSide::Down) if imbalance > Decimal::ZERO => {
                /* Need more Down to match Up - allow extra room */
                (up_room, down_room.max(rebalance_room))
            }
            _ => (up_room, down_room),
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
            /* 
             * REBALANCING: Single-side orders to equalize position.
             * Now validated against combined cost using avg fill prices.
             * Will only place orders if: price + other_side_avg < max_pair_cost
             */
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
        imbalance: Decimal,
        allow_imbalance_side: Option<MarketSide>,
        _overrides: &LadderOverrides,
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
        
        /*
         * AGGRESSIVE PRICING for faster fills:
         * High conviction → small offset (bid close to ask, fill fast)
         * Low conviction → larger offset (more buffer, fill slowly)
         * 
         * conviction = 1.0 → offset = base * 0.5 (very aggressive, 0.5¢ from ask)
         * conviction = 0.5 → offset = base * 1.0 (normal)
         * conviction = 0.0 → offset = base * 1.5 (conservative)
         */
        let conviction = overrides.conviction.clamp(0.0, 1.0);
        let aggression_factor = Decimal::from_f64(1.5 - conviction).unwrap_or(Decimal::ONE);
        let base_offset = (self.config.top_offset + overrides.extra_offset) * aggression_factor;
        
        /* Minimum offset of 0.5¢ to avoid crossing the spread */
        let base_offset = base_offset.max(dec!(0.005));
        
        /*
         * Momentum-aware asymmetric pricing:
         * - When Up trending up (momentum > 0): bid tight on Up (elusive), wide on Down (cheap)
         * - When Up trending down (momentum < 0): bid tight on Down (elusive), wide on Up (cheap)
         * - The "elusive" side is chased aggressively, the "cheap" side we wait for
         */
        const MOMENTUM_THRESHOLD: f64 = 0.3;
        
        let (up_offset, down_offset) = if overrides.momentum > MOMENTUM_THRESHOLD {
            /* Up is elusive (trending up), Down is cheap (will get cheaper) */
            (base_offset, base_offset * PATIENCE_MULT)
        } else if overrides.momentum < -MOMENTUM_THRESHOLD {
            /* Down is elusive (trending up), Up is cheap */
            (base_offset * PATIENCE_MULT, base_offset)
        } else {
            /* Stable market - bid normally on both */
            (base_offset, base_offset)
        };
        
        /*
         * EXTREME MARKET DETECTION
         * When one side is very cheap (< 10¢) and the other is very expensive (> 90¢):
         * - Cheap side will fill instantly
         * - Expensive side is "elusive" - might not fill or keep going up
         * 
         * Strategy: ONLY buy the elusive side with aggressive pricing (at ask - 1¢).
         * Skip the cheap side entirely in paired orders.
         * Once elusive side fills (creating imbalance), rebalancing will get cheap side.
         */
        const EXTREME_CHEAP_THRESHOLD: Decimal = dec!(0.10);
        const EXTREME_EXPENSIVE_THRESHOLD: Decimal = dec!(0.90);
        
        let extreme_elusive_side: Option<MarketSide> = 
            if down_ask < EXTREME_CHEAP_THRESHOLD && up_ask > EXTREME_EXPENSIVE_THRESHOLD {
                info!(
                    up_ask = %up_ask,
                    down_ask = %down_ask,
                    "Extreme market - buying elusive UP first, will get cheap Down later"
                );
                Some(MarketSide::Up)
            } else if up_ask < EXTREME_CHEAP_THRESHOLD && down_ask > EXTREME_EXPENSIVE_THRESHOLD {
                info!(
                    up_ask = %up_ask,
                    down_ask = %down_ask,
                    "Extreme market - buying elusive DOWN first, will get cheap Up later"
                );
                Some(MarketSide::Down)
            } else {
                None
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
             * EXTREME MARKET HANDLING:
             * Only place orders for the elusive side. Rebalancing will pick up cheap side.
             */
            if let Some(elusive) = extreme_elusive_side {
                let (elusive_price, elusive_ask) = match elusive {
                    MarketSide::Up => (price_up, up_ask),
                    MarketSide::Down => (price_down, down_ask),
                };
                /* Aggressive pricing: bid at ask - 1¢ (or use price_cap if tighter) */
                let aggressive_price = floor_to_tick((elusive_ask - dec!(0.01)).max(dec!(0.01)), tick);
                let final_price = aggressive_price.min(elusive_price);
                
                orders.push(LadderOrder {
                    side: elusive,
                    price: final_price,
                    size,
                });
                remaining_room -= size;
                continue;  /* Skip the cheap side entirely */
            }
            
            /* 
             * NORMAL MARKET: Buy cheaper side first
             * The cheaper side is more likely to move away from us.
             */
            if price_down < price_up {
                /* Down is cheaper → buy Down first */
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
                /* Up is cheaper or equal → buy Up first */
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
        
        /* 
         * COMBINED COST VALIDATION FOR SINGLE-SIDE ORDERS
         * 
         * When rebalancing (buying one side to match the other), we must ensure
         * the combined cost will still be profitable.
         * 
         * Example: If we have Up at avg 45¢, we can pay at most 53¢ for Down
         * to keep combined < 98¢.
         */
        let other_side_avg = match side {
            MarketSide::Up => overrides.avg_down_cost,
            MarketSide::Down => overrides.avg_up_cost,
        };
        
        /* If we don't know the other side's cost, we can't safely rebalance */
        let other_avg = match other_side_avg {
            Some(avg) if avg > Decimal::ZERO => avg,
            _ => {
                debug!(
                    side = ?side,
                    "Cannot rebalance - no tracked cost for other side"
                );
                return orders;
            }
        };
        
        /* 
         * Calculate max price we can pay.
         * For HEDGING/REBALANCING, we allow a small loss to reduce directional risk.
         * max_for_combined = (1.0 + max_hedge_loss) - other_avg
         * 
         * Example: other_avg=0.36, max_hedge_loss=0.03
         * max_for_combined = 1.03 - 0.36 = 0.67 (can pay up to 67¢, accepting 3¢ loss)
         */
        let max_combined_with_loss = Decimal::ONE + self.config.max_hedge_loss;
        let max_for_combined = max_combined_with_loss - other_avg;
        if max_for_combined <= dec!(0.01) {
            debug!(
                side = ?side,
                other_avg = %other_avg,
                max_combined = %max_combined_with_loss,
                "Cannot rebalance - other side too expensive even with hedge tolerance"
            );
            return orders;
        }
        
        /* Check if we have edge on this side (price cap exists) */
        let price_cap = match side {
            MarketSide::Up => overrides.up_price_cap,
            MarketSide::Down => overrides.down_price_cap,
        };
        
        /* Use the more restrictive of: edge-based cap or combined-cost cap */
        let cap = match price_cap {
            Some(edge_cap) => edge_cap.min(max_for_combined),
            None => max_for_combined,
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
        let tick = self.config.tick_size.max(dec!(0.01));
        let levels = overrides.max_levels.unwrap_or(self.config.levels);
        
        /* 
         * In EXTREME MARKETS (cheap side < 10¢), bid more aggressively.
         * The cheap side won't get much cheaper, and we have the elusive side locked in.
         */
        let current_ask = match side {
            MarketSide::Up => up_ask,
            MarketSide::Down => down_ask,
        };
        let is_cheap_side = current_ask < dec!(0.10);
        let top_offset = if is_cheap_side {
            /* Aggressive: 0.5¢ offset for cheap side (near ask) */
            dec!(0.005)
        } else {
            self.config.top_offset + overrides.extra_offset
        };

        let top_price = match side {
            MarketSide::Up => (up_ask - top_offset).max(dec!(0.01)).min(cap),
            MarketSide::Down => (down_ask - top_offset).max(dec!(0.01)).min(cap),
        };
        
        if is_cheap_side {
            info!(
                side = ?side,
                ask = %current_ask,
                top_price = %top_price,
                other_avg = %other_avg,
                combined = %(top_price + other_avg),
                "Aggressive rebalance on cheap side"
            );
        }

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
            
            /* Validate combined cost for each order (with hedge tolerance) */
            let combined = price + other_avg;
            if combined > max_combined_with_loss {
                debug!(
                    side = ?side,
                    price = %price,
                    other_avg = %other_avg,
                    combined = %combined,
                    max = %max_combined_with_loss,
                    "Skipping rebalance order - combined cost too high even with hedge tolerance"
                );
                continue;
            }

            let size = round_size(size_per_level.min(remaining_room));
            if size < MIN_ORDER_SIZE {
                break;
            }
            
            let profit_or_loss = if combined <= Decimal::ONE {
                format!("+{:.1}¢", (Decimal::ONE - combined) * dec!(100))
            } else {
                format!("-{:.1}¢ (hedge)", (combined - Decimal::ONE) * dec!(100))
            };
            
            info!(
                side = ?side,
                price = %price,
                other_avg = %other_avg,
                combined = %combined,
                pnl = %profit_or_loss,
                size = %size,
                "Rebalance order validated"
            );

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
    /* Conviction: 0-1 confidence in current market state */
    pub conviction: f64,
    /* Average fill prices for each side (for single-side combined cost validation) */
    pub avg_up_cost: Option<Decimal>,
    pub avg_down_cost: Option<Decimal>,
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
            conviction: 0.5,  /* Default moderate conviction */
            max_levels: None,
            allow_imbalance_side: None,
            avg_up_cost: None,
            avg_down_cost: None,
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
