// ladder.rs
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::collections::HashSet;
use tracing::{debug, info};

use crate::{
    constants::round_size,
    poller::{MarketPosition, MarketSide},
};
const MIN_ORDER_SIZE: Decimal = dec!(5);

/// Configuration for dynamic order sizing based on edge
#[derive(Debug, Clone)]
pub struct DynamicSizingConfig {
    /// Base size per level when edge is minimal
    pub base_size: Decimal,
    /// Minimum edge in basis points to trade at all
    pub min_edge_bps: i32,
    /// Size multiplier per 100 basis points of edge
    pub edge_scale_factor: Decimal,
    /// Maximum size multiplier (cap)
    pub max_size_multiplier: Decimal,
    /// Never take more than this percentage of available liquidity
    pub liquidity_cap_pct: Decimal,
}

impl Default for DynamicSizingConfig {
    fn default() -> Self {
        Self {
            base_size: dec!(10),
            min_edge_bps: 0,  // Trade even at 0 edge (market making)
            edge_scale_factor: dec!(0.5),  // +50% size per 1% edge
            max_size_multiplier: dec!(3),  // Max 3x base size
            liquidity_cap_pct: dec!(0.5),  // Never take more than 50% of depth
        }
    }
}

pub struct LadderConfig {
    pub levels: usize,
    pub spacing: Decimal,
    pub size_per_level: Decimal,
    pub shares_per_round: Decimal,
    pub reladder_threshold: Decimal,
    pub stale_order_distance: Decimal,
    
    // Edge creation - THIS IS THE KEY
    /// Target combined cost for a pair (Up bid + Down bid)
    /// Must be < 1.00 to guarantee profit. Lower = more edge but fewer fills.
    pub target_combined_cost: Decimal,
    /// Minimum offset from ask (even if target allows closer)
    pub min_offset: Decimal,
    
    // Round-based safety settings
    pub max_rounds_per_market: usize,
    pub max_unpaired_exposure: Decimal,
    pub min_seconds_before_end: i64,

    // Dynamic sizing configuration
    pub dynamic_sizing: DynamicSizingConfig,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            levels: 4,                          // More levels to catch fills at low prices
            spacing: dec!(0.01),                // 1¢ between levels
            size_per_level: dec!(10),           // Standard size
            shares_per_round: dec!(30),         // Reasonable round size
            reladder_threshold: dec!(0.02),     // Don't fuss over small moves
            stale_order_distance: dec!(0.10),   // Let orders sit - we WANT low fills
            
            // Edge creation - bid LOW to create room for chasing
            target_combined_cost: dec!(0.96),   // 4% buffer - room to chase if needed
            min_offset: dec!(0.02),             // At least 2¢ below ask
            
            // Round-based safety
            max_rounds_per_market: 100,         // Many rounds
            max_unpaired_exposure: dec!(10),    // Stay balanced
            min_seconds_before_end: 60,         // Stop 1 minute before end

            // Dynamic sizing
            dynamic_sizing: DynamicSizingConfig::default(),
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

    /// Compute dynamic order size based on edge and liquidity
    pub fn compute_dynamic_size(
        &self,
        edge_bps: i32,
        available_liquidity: Decimal,
    ) -> Decimal {
        let ds = &self.config.dynamic_sizing;
        
        // Check minimum edge threshold
        if edge_bps < ds.min_edge_bps {
            return ds.base_size;
        }

        // Scale up based on edge: base * (1 + edge_pct * scale_factor)
        let edge_pct = Decimal::from(edge_bps) / dec!(100);  // Convert bps to percentage
        let multiplier = dec!(1) + edge_pct * ds.edge_scale_factor;
        let capped_multiplier = multiplier.min(ds.max_size_multiplier);
        
        let edge_scaled = ds.base_size * capped_multiplier;
        
        // Cap at liquidity percentage
        let liq_cap = available_liquidity * ds.liquidity_cap_pct;
        let final_size = edge_scaled.min(liq_cap);

        round_size(final_size.max(MIN_ORDER_SIZE))
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
        self.compute_ladder_with_urgency(
            up_ask,
            down_ask,
            position,
            pending_up,
            pending_down,
            open_orders,
            locked_pairs,
            max_up_price,
            max_down_price,
            None, // No urgency by default
        )
    }

    /* Compute ladder with hedge urgency awareness */
    pub fn compute_ladder_with_urgency(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        position: &MarketPosition,
        pending_up: Decimal,
        pending_down: Decimal,
        open_orders: &[OpenOrderInfo],
        locked_pairs: Decimal,
        max_up_price: Option<Decimal>,
        max_down_price: Option<Decimal>,
        urgency: Option<&crate::pair_tracker::HedgeUrgency>,
    ) -> LadderPlan {
        let mut plan = LadderPlan::default();

        // Subtract locked pairs from position - those are "done"
        let working_up = (position.up_shares - locked_pairs).max(Decimal::ZERO);
        let working_down = (position.down_shares - locked_pairs).max(Decimal::ZERO);
        
        // Determine which side is ELUSIVE (expensive = hard to fill) vs EASY (cheap = easy to fill)
        // The expensive side has fewer sellers, so we should get that FIRST with passive orders
        // Then chase the cheap side to complete pairs
        let up_is_elusive = up_ask > down_ask;
        
        // AGGRESSIVE REBALANCING: Chase the EASY side to complete pairs
        // Only chase if we already have the ELUSIVE side filled
        let unpaired_up = (working_up - working_down).max(Decimal::ZERO);
        let unpaired_down = (working_down - working_up).max(Decimal::ZERO);
        
        if unpaired_up > Decimal::ZERO && up_is_elusive {
            // We have extra Up (elusive) shares - chase Down (easy) to complete
            let chase_size = unpaired_up.min(self.config.size_per_level);
            if let Some(max_price) = max_down_price {
                if down_ask <= max_price {
                    info!(
                        unpaired = %unpaired_up,
                        chase_size = %chase_size,
                        down_ask = %down_ask,
                        "🎯 Chasing Down (easy side) to complete pairs"
                    );
                    plan.orders.push(LadderOrder {
                        side: MarketSide::Down,
                        price: down_ask,  // HIT THE ASK
                        size: round_size(chase_size),
                    });
                }
            }
        }
        
        if unpaired_down > Decimal::ZERO && !up_is_elusive {
            // We have extra Down (elusive) shares - chase Up (easy) to complete
            let chase_size = unpaired_down.min(self.config.size_per_level);
            if let Some(max_price) = max_up_price {
                if up_ask <= max_price {
                    info!(
                        unpaired = %unpaired_down,
                        chase_size = %chase_size,
                        up_ask = %up_ask,
                        "🎯 Chasing Up (easy side) to complete pairs"
                    );
                    plan.orders.push(LadderOrder {
                        side: MarketSide::Up,
                        price: up_ask,  // HIT THE ASK
                        size: round_size(chase_size),
                    });
                }
            }
        }
        
        // If we have unpaired shares on the EASY side, don't chase - place passive orders on elusive side instead
        // This handles the case where we accidentally got the easy side first
        
        // EDGE CREATION: Calculate bid prices that sum to target_combined_cost
        // This guarantees profit if both sides fill
        let (up_bid, down_bid) = self.compute_edge_creating_bids(up_ask, down_ask, urgency);
        
        debug!(
            up_ask = %up_ask,
            down_ask = %down_ask,
            up_bid = %up_bid,
            down_bid = %down_bid,
            edge = %(dec!(1.00) - up_bid - down_bid),
            "Bid prices"
        );

        // Compute edge for sizing decisions (based on our BID prices, not asks)
        let edge = dec!(1.00) - up_bid - down_bid;
        let edge_bps = (edge * dec!(10000))
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0) as i32;

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

        // 6. Generate ladders at edge-creating bid prices
        // PRIORITIZE THE ELUSIVE SIDE - place more orders there, fewer on easy side
        let up_is_elusive = up_ask > down_ask;
        
        // Calculate room adjustment: more room for elusive side, less for easy side
        let (adjusted_up_room, adjusted_down_room) = if up_is_elusive {
            // Up is elusive - give it full room, limit easy side
            let easy_limit = unpaired_up.max(self.config.size_per_level); // Only chase what we need
            (up_room, down_room.min(easy_limit))
        } else {
            // Down is elusive - give it full room, limit easy side
            let easy_limit = unpaired_down.max(self.config.size_per_level);
            (up_room.min(easy_limit), down_room)
        };
        
        if adjusted_up_room >= MIN_ORDER_SIZE {
            for order in self.generate_ladder_from_bid(
                MarketSide::Up,
                up_bid,
                up_ask,
                adjusted_up_room,
                max_up_price,
                edge_bps,
            ) {
                if !covered_up.contains(&order.price) {
                    plan.orders.push(order);
                }
            }
        }

        if adjusted_down_room >= MIN_ORDER_SIZE {
            for order in self.generate_ladder_from_bid(
                MarketSide::Down,
                down_bid,
                down_ask,
                adjusted_down_room,
                max_down_price,
                edge_bps,
            ) {
                if !covered_down.contains(&order.price) {
                    plan.orders.push(order);
                }
            }
        }

        plan
    }
    
    /// Generate a ladder starting from a computed bid price
    fn generate_ladder_from_bid(
        &self,
        side: MarketSide,
        top_bid: Decimal,
        ask: Decimal,
        room: Decimal,
        max_price: Option<Decimal>,
        edge_bps: i32,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();

        if room <= Decimal::ZERO {
            return orders;
        }

        // Don't place bids at or above the ask
        if top_bid >= ask {
            return orders;
        }
        
        // Respect max hedge price if we have unpaired exposure
        let effective_max = max_price.unwrap_or(dec!(0.99));
        if top_bid > effective_max {
            // Skip quietly - don't chase unprofitable hedges
            return orders;
        }

        // Compute dynamic size based on edge
        let dynamic_size = self.compute_dynamic_size(edge_bps, room);
        let size_per_level = dynamic_size.min(self.config.size_per_level);

        let mut remaining_room = room;

        // Place orders at multiple levels below top_bid
        for level in 0..self.config.levels {
            let price = (top_bid - self.config.spacing * Decimal::from(level as u32)).max(dec!(0.01));

            if price >= ask {
                continue;
            }
            
            if price > effective_max {
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

    /// Compute bid prices that CREATE edge by summing to target_combined_cost
    /// This is the core of the market making strategy
    fn compute_edge_creating_bids(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        urgency: Option<&crate::pair_tracker::HedgeUrgency>,
    ) -> (Decimal, Decimal) {
        let target = self.config.target_combined_cost;
        let min_offset = self.config.min_offset;
        let combined_ask = up_ask + down_ask;
        
        // Calculate how much total offset we need from asks to hit target
        // If combined_ask = 1.02 and target = 0.97, we need 0.05 total offset
        let total_offset_needed = (combined_ask - target).max(dec!(0.02));
        
        // Split offset proportionally - cheaper side gets proportionally less offset
        // This puts bids at similar "distance" from each side's fair value
        let up_proportion = up_ask / combined_ask;
        let down_proportion = down_ask / combined_ask;
        
        let mut up_offset = (total_offset_needed * up_proportion).max(min_offset);
        let mut down_offset = (total_offset_needed * down_proportion).max(min_offset);
        
        // Adjust for urgency - if we need one side urgently, reduce its offset
        if let Some(u) = urgency {
            if u.is_high() {
                use crate::poller::MarketSide;
                
                let urgency_factor = if u.is_critical() {
                    dec!(0.25)  // 75% reduction
                } else {
                    dec!(0.5)   // 50% reduction
                };
                
                match u.needs_side {
                    MarketSide::Up => {
                        let old_offset = up_offset;
                        up_offset = (up_offset * urgency_factor).max(dec!(0.005));
                        // Compensate on the other side to maintain total edge
                        down_offset = down_offset + (old_offset - up_offset);
                    }
                    MarketSide::Down => {
                        let old_offset = down_offset;
                        down_offset = (down_offset * urgency_factor).max(dec!(0.005));
                        // Compensate on the other side to maintain total edge
                        up_offset = up_offset + (old_offset - down_offset);
                    }
                }
            }
        }
        
        // Calculate final bid prices
        let up_bid = (up_ask - up_offset).max(dec!(0.01));
        let down_bid = (down_ask - down_offset).max(dec!(0.01));
        
        (up_bid, down_bid)
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
                // Quietly mark for cancellation - no need to spam logs
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
        self.generate_side_ladder_with_offset(side, ask, room, max_price, 0, self.config.min_offset)
    }

    /// Generate ladder with dynamic sizing based on edge
    fn generate_side_ladder_dynamic(
        &self,
        side: MarketSide,
        ask: Decimal,
        room: Decimal,
        max_price: Option<Decimal>,
        edge_bps: i32,
    ) -> Vec<LadderOrder> {
        self.generate_side_ladder_with_offset(side, ask, room, max_price, edge_bps, self.config.min_offset)
    }

    /// Generate ladder with custom offset (for urgency-aware placement)
    fn generate_side_ladder_with_offset(
        &self,
        side: MarketSide,
        ask: Decimal,
        room: Decimal,
        max_price: Option<Decimal>,
        edge_bps: i32,
        top_offset: Decimal,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();

        if room <= Decimal::ZERO {
            return orders;
        }

        // If we have unpaired shares on the opposite side, we can only buy at max_price
        // to stay profitable. If market has moved past this, skip placing orders.
        let effective_max = max_price.unwrap_or(dec!(0.99));
        // Use the passed top_offset (may be adjusted for urgency)
        let top_price = (ask - top_offset).max(dec!(0.01));
        
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

        // Compute dynamic size based on edge
        // Use room as a proxy for available liquidity
        let dynamic_size = self.compute_dynamic_size(edge_bps, room);
        

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

            let size = round_size(dynamic_size.min(remaining_room));
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
