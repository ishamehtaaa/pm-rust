/* ladder.rs - Order ladder generation and management */
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::{debug, info, trace};

/* Patience multiplier for momentum-aware pricing (reduced from 2.0 to avoid asymmetric exposure) */
const PATIENCE_MULT: Decimal = dec!(1.3);
/* Chase multiplier for elusive side - less aggressive to reduce asymmetric fill risk (was 0.5) */
const CHASE_MULT: Decimal = dec!(0.8);

use crate::{
    constants::round_size,
    poller::{MarketPosition, MarketSide},
};

pub struct LadderConfig {
    pub levels: usize,
    pub spacing: Decimal,
    pub size_per_level: Decimal,
    pub min_order_size: Decimal,
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
            // Polymarket enforces a minimum order size (typically 5 shares for these markets).
            // Never generate orders smaller than this or the API will reject them.
            min_order_size: dec!(5),
            top_offset: dec!(0.02),
            max_pair_cost: dec!(0.96),  /* Tighter cap to ensure profitability even with execution slippage */
            tick_size: dec!(0.01),
            target_per_side: dec!(10),
            max_position_per_side: dec!(50),
            max_pending_per_side: dec!(20),
            reladder_threshold: dec!(0.02),
            stale_order_distance: dec!(0.04),  /* Tighter: cancel orders faster when market moves away */
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
        let min_order_size = self.config.min_order_size.max(dec!(0.01));

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
        if up_room >= min_order_size && down_room >= min_order_size {
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
        let imbalance_abs = imbalance.abs();
        let underweight_side = if imbalance > Decimal::ZERO {
            Some(MarketSide::Down) // Heavy Up → need Down
        } else if imbalance < Decimal::ZERO {
            Some(MarketSide::Up) // Heavy Down → need Up
        } else {
            None
        };

        for order in open_orders {
            let current_ask = match order.side {
                MarketSide::Up => up_ask,
                MarketSide::Down => down_ask,
            };

            /* Cancel if too far from market */
            let distance = current_ask - order.price;
            if distance > self.config.stale_order_distance {
                // If we're materially imbalanced, keep (don't cancel) deeper orders on the
                // underweight side so we can still get hedging fills when price reverts.
                let protect_for_rebalance = underweight_side == Some(order.side)
                    && imbalance_abs >= self.config.max_imbalance * dec!(0.5);
                if protect_for_rebalance {
                    debug!(
                        order_id = %order.order_id,
                        side = ?order.side,
                        price = %order.price,
                        current_ask = %current_ask,
                        distance = %distance,
                        imbalance = %imbalance,
                        "Keeping underweight-side order despite distance (rebalance hedge)"
                    );
                    continue;
                }
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
        let base_offset = self.config.top_offset + overrides.extra_offset;
        let min_order_size = self.config.min_order_size.max(dec!(0.01));

        /*
         * Price cap logic with fallback for rebalancing:
         * - If we have a model-derived cap, use it
         * - If no cap but we need to rebalance (allow_imbalance_side), use market_bid as fallback
         * - Otherwise skip that side
         */
        let up_cap = match overrides.up_price_cap {
            Some(cap) => cap,
            None => {
                // No model edge - use fallback price (bid at market_ask - offset)
                // This is conservative but ensures we have orders in the book
                let fallback = (up_ask - base_offset).max(dec!(0.01));
                debug!(
                    up_ask = %up_ask,
                    fallback = %fallback,
                    "No Up edge from model, using fallback price"
                );
                fallback
            }
        };
        let down_cap = match overrides.down_price_cap {
            Some(cap) => cap,
            None => {
                let fallback = (down_ask - base_offset).max(dec!(0.01));
                debug!(
                    down_ask = %down_ask,
                    fallback = %fallback,
                    "No Down edge from model, using fallback price"
                );
                fallback
            }
        };

        let mut remaining_room = up_room.min(down_room);
        if remaining_room <= Decimal::ZERO {
            return orders;
        }

        let size_per_level =
            (self.config.size_per_level * overrides.size_multiplier).max(min_order_size);
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

        /*
         * INVENTORY-AWARE SKEW (equalization):
         * Use current inventory imbalance to bias quoting:
         * - Heavy Up (imbalance > 0): make Up bids more passive, Down bids more aggressive.
         * - Heavy Down (imbalance < 0): make Down bids more passive, Up bids more aggressive.
         *
         * We derive imbalance from remaining room:
         *   imbalance = total_up - total_down = down_room - up_room
         */
        let imbalance = down_room - up_room;
        let max_imbalance = self.config.max_imbalance.abs();
        let strength = if max_imbalance > Decimal::ZERO {
            (imbalance.abs() / max_imbalance).min(Decimal::ONE)
        } else {
            Decimal::ZERO
        };

        let chase_adjust = (Decimal::ONE - CHASE_MULT) * strength; // 0..0.2
        let patience_adjust = (PATIENCE_MULT - Decimal::ONE) * strength; // 0..0.3

        let (inv_up_mult, inv_down_mult) = if imbalance > Decimal::ZERO {
            // Heavy Up → avoid filling Up, chase Down
            (Decimal::ONE + patience_adjust, Decimal::ONE - chase_adjust)
        } else if imbalance < Decimal::ZERO {
            // Heavy Down → avoid filling Down, chase Up
            (Decimal::ONE - chase_adjust, Decimal::ONE + patience_adjust)
        } else {
            (Decimal::ONE, Decimal::ONE)
        };

        let up_offset = up_offset * inv_up_mult;
        let down_offset = down_offset * inv_down_mult;
        
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
            if size < min_order_size {
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
        let base_offset = self.config.top_offset + overrides.extra_offset;
        let min_order_size = self.config.min_order_size.max(dec!(0.01));
        
        /* Check if we have edge on this side (price cap exists) */
        let price_cap = match side {
            MarketSide::Up => overrides.up_price_cap,
            MarketSide::Down => overrides.down_price_cap,
        };
        
        let cap = match price_cap {
            Some(cap) => cap,
            None => {
                // No model edge - use fallback (ask - offset) for rebalancing
                let ask = match side {
                    MarketSide::Up => up_ask,
                    MarketSide::Down => down_ask,
                };
                let fallback = (ask - base_offset).max(dec!(0.01));
                debug!(side = ?side, ask = %ask, fallback = %fallback, "No edge, using fallback price");
                fallback
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
            (self.config.size_per_level * overrides.size_multiplier).max(min_order_size);
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
            if size < min_order_size {
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
