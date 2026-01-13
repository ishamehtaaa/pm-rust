// ladder_v3.rs
//
// Patient ladder strategy with time-aware rebalancing.
//
// Key principles:
// 1. Don't cancel unless truly stale (far from current price)
// 2. Don't aggressively rebalance unless truly imbalanced OR running out of time
// 3. Accept a bad pair if it lets us hunt for a cheap recovery pair
// 4. Track average cost across all pairs, not per-pair

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use tracing::{debug, info, trace, warn};

use crate::poller::{MarketPosition, MarketSide};

/* The structure of an open order. Used to help categorize stale orders. */
#[derive(Debug, Clone)]
pub struct OpenOrderInfo {
    pub order_id: String,
    pub side: MarketSide,
    pub price: Decimal,
    pub remaining_size: Decimal,
}

#[derive(Debug, Clone)]
pub struct LadderConfig {
    /// Number of price levels per side
    pub levels: usize,
    /// Price spacing between levels
    pub spacing: Decimal,
    /// Base size per level
    pub base_size_per_level: Decimal,
    /// Offset from best ask for top rung
    pub top_offset: Decimal,
    /// Target shares per side
    pub target_per_side: Decimal,
    /// Minimum order size
    pub min_order_size: Decimal,

    // === Cancellation settings (BE PATIENT) ===
    /// Only cancel if this far from current price (e.g., 0.12 = 12 cents)
    pub stale_order_distance: Decimal,
    /// Ticks to confirm staleness before cancelling
    pub stale_hysteresis: u32,

    // === Reladder settings (ADD, DON'T REPLACE) ===
    /// Price move to trigger adding new orders (not cancelling old ones)
    pub reladder_threshold: Decimal,

    // === Rebalance settings (BE PATIENT, THEN URGENT) ===
    /// Imbalance threshold for aggressive rebalance during normal trading
    pub urgent_imbalance_threshold: Decimal,
    /// Time remaining (seconds) when we get more aggressive about rebalancing
    pub late_game_seconds: i64,
    /// Imbalance threshold during late game (lower = more aggressive)
    pub late_game_imbalance_threshold: Decimal,
    /// Maximum we'll pay above target for a rebalance (will hunt cheap pair after)
    pub max_rebalance_premium: Decimal,

    // === Cost tracking ===
    /// Target average cost per pair
    pub target_avg_pair_cost: Decimal,
    /// After a bad pair, how much cheaper must recovery pair be
    pub recovery_pair_discount: Decimal,
}

impl Default for LadderConfig {
    fn default() -> Self {
        Self {
            levels: 3,
            spacing: dec!(0.01),
            base_size_per_level: dec!(5),
            top_offset: dec!(0.01),
            target_per_side: dec!(25),
            min_order_size: dec!(5),

            // Patient cancellation
            stale_order_distance: dec!(0.12), // Only cancel if 12+ cents away
            stale_hysteresis: 3,

            // Add orders on price moves, don't replace
            reladder_threshold: dec!(0.03),

            // Rebalance settings
            urgent_imbalance_threshold: dec!(20), // Very patient normally
            late_game_seconds: 180,               // 3 minutes
            late_game_imbalance_threshold: dec!(10), // More aggressive late
            max_rebalance_premium: dec!(0.04),    // Pay up to 4 cents over target

            // Cost tracking
            target_avg_pair_cost: dec!(0.98),
            recovery_pair_discount: dec!(0.04), // Recovery pair must be 4 cents cheaper
        }
    }
}

// ============================================================================
// Cost Tracking
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct CostTracker {
    /// Completed pairs: (up_price, down_price)
    pairs: Vec<(Decimal, Decimal)>,
    /// Unpaired fills: side -> Vec<(price, size)>
    unpaired_up: Vec<(Decimal, Decimal)>,
    unpaired_down: Vec<(Decimal, Decimal)>,
}

impl CostTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fill
    pub fn record_fill(&mut self, side: MarketSide, price: Decimal, size: Decimal) {
        match side {
            MarketSide::Up => self.unpaired_up.push((price, size)),
            MarketSide::Down => self.unpaired_down.push((price, size)),
        }
        self.match_pairs();
    }

    /// Match unpaired fills into pairs (FIFO)
    fn match_pairs(&mut self) {
        while !self.unpaired_up.is_empty() && !self.unpaired_down.is_empty() {
            let (up_price, mut up_size) = self.unpaired_up[0];
            let (down_price, mut down_size) = self.unpaired_down[0];

            let pair_size = up_size.min(down_size);

            // Record the pair
            self.pairs.push((up_price, down_price));

            // Reduce sizes
            up_size -= pair_size;
            down_size -= pair_size;

            if up_size <= Decimal::ZERO {
                self.unpaired_up.remove(0);
            } else {
                self.unpaired_up[0].1 = up_size;
            }

            if down_size <= Decimal::ZERO {
                self.unpaired_down.remove(0);
            } else {
                self.unpaired_down[0].1 = down_size;
            }
        }
    }

    /// Average cost of completed pairs
    pub fn avg_pair_cost(&self) -> Option<Decimal> {
        if self.pairs.is_empty() {
            return None;
        }
        let total: Decimal = self.pairs.iter().map(|(u, d)| u + d).sum();
        Some(total / Decimal::from(self.pairs.len()))
    }

    /// Number of completed pairs
    pub fn pair_count(&self) -> usize {
        self.pairs.len()
    }

    /// Current imbalance in shares (positive = long UP)
    pub fn unpaired_imbalance(&self) -> Decimal {
        let up_shares: Decimal = self.unpaired_up.iter().map(|(_, s)| s).sum();
        let down_shares: Decimal = self.unpaired_down.iter().map(|(_, s)| s).sum();
        up_shares - down_shares
    }

    /// Average price of unpaired fills on a side
    pub fn avg_unpaired_price(&self, side: MarketSide) -> Option<Decimal> {
        let fills = match side {
            MarketSide::Up => &self.unpaired_up,
            MarketSide::Down => &self.unpaired_down,
        };

        if fills.is_empty() {
            return None;
        }

        let total_cost: Decimal = fills.iter().map(|(p, s)| p * s).sum();
        let total_size: Decimal = fills.iter().map(|(_, s)| s).sum();

        if total_size > Decimal::ZERO {
            Some(total_cost / total_size)
        } else {
            None
        }
    }

    /// If we rebalance at this price, what would our new average be?
    pub fn projected_avg_if_rebalance(
        &self,
        rebalance_price: Decimal,
        side: MarketSide,
    ) -> Decimal {
        let current_total: Decimal = self.pairs.iter().map(|(u, d)| u + d).sum();
        let current_count = self.pairs.len();

        // The unpaired side's average price
        let unpaired_price = match side {
            MarketSide::Up => self
                .avg_unpaired_price(MarketSide::Down)
                .unwrap_or(dec!(0.50)),
            MarketSide::Down => self
                .avg_unpaired_price(MarketSide::Up)
                .unwrap_or(dec!(0.50)),
        };

        let new_pair_cost = rebalance_price + unpaired_price;
        let new_total = current_total + new_pair_cost;
        let new_count = current_count + 1;

        if new_count > 0 {
            new_total / Decimal::from(new_count)
        } else {
            new_pair_cost
        }
    }

    /// What price do we need on a recovery pair to get back to target average?
    pub fn required_recovery_pair_cost(&self, target_avg: Decimal) -> Decimal {
        let current_total: Decimal = self.pairs.iter().map(|(u, d)| u + d).sum();
        let current_count = self.pairs.len();

        // (current_total + recovery_cost) / (current_count + 1) = target_avg
        // recovery_cost = target_avg * (current_count + 1) - current_total
        target_avg * Decimal::from(current_count + 1) - current_total
    }
}

// ============================================================================
// Market Phase
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MarketPhase {
    /// Plenty of time - be patient
    Normal,
    /// Running low on time - more aggressive
    LateGame,
    /// Almost over - close out or accept fate
    Final,
}

impl MarketPhase {
    pub fn from_seconds_remaining(seconds: i64, late_game_threshold: i64) -> Self {
        if seconds <= 30 {
            MarketPhase::Final
        } else if seconds <= late_game_threshold {
            MarketPhase::LateGame
        } else {
            MarketPhase::Normal
        }
    }
}

// ============================================================================
// Ladder State
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReladderReason {
    Initial,
    PriceMove,
    InventoryChange,
    LateGameRebalance,
}

#[derive(Debug, Default)]
pub struct LadderState {
    last_prices: HashMap<String, (Decimal, Decimal)>,
    last_inventory: HashMap<String, (Decimal, Decimal)>,
    stale_counts: HashMap<String, u32>,
    last_reladder_ms: HashMap<String, u64>,
}

impl LadderState {
    pub fn should_reladder(
        &self,
        market_id: &str,
        up_ask: Decimal,
        down_ask: Decimal,
        position: &MarketPosition,
        config: &LadderConfig,
        _now_ms: u64,
    ) -> Option<ReladderReason> {
        // Initial placement
        if self.last_prices.get(market_id).is_none() {
            return Some(ReladderReason::Initial);
        }

        // Inventory changed (we got filled)
        if let Some((last_up, last_down)) = self.last_inventory.get(market_id) {
            if *last_up != position.up_shares || *last_down != position.down_shares {
                return Some(ReladderReason::InventoryChange);
            }
        }

        // Price moved enough to ADD new orders
        if let Some((last_up_ask, last_down_ask)) = self.last_prices.get(market_id) {
            let up_diff = (up_ask - *last_up_ask).abs();
            let down_diff = (down_ask - *last_down_ask).abs();
            if up_diff >= config.reladder_threshold || down_diff >= config.reladder_threshold {
                return Some(ReladderReason::PriceMove);
            }
        }

        None
    }

    pub fn record_ladder(
        &mut self,
        market_id: String,
        up_ask: Decimal,
        down_ask: Decimal,
        position: &MarketPosition,
        now_ms: u64,
    ) {
        self.last_prices
            .insert(market_id.clone(), (up_ask, down_ask));
        self.last_inventory.insert(
            market_id.clone(),
            (position.up_shares, position.down_shares),
        );
        self.last_reladder_ms.insert(market_id, now_ms);
    }

    /// Only cancel orders that are TRULY stale (far from current price)
    pub fn stale_cancellations(
        &mut self,
        open_orders: &[OpenOrderInfo],
        up_ask: Decimal,
        down_ask: Decimal,
        config: &LadderConfig,
    ) -> Vec<String> {
        let mut to_cancel = Vec::new();
        let mut still_open: HashSet<String> = HashSet::new();

        for order in open_orders {
            still_open.insert(order.order_id.clone());

            let current_ask = match order.side {
                MarketSide::Up => up_ask,
                MarketSide::Down => down_ask,
            };

            let distance = (current_ask - order.price).abs();

            if distance > config.stale_order_distance {
                let count = self.stale_counts.entry(order.order_id.clone()).or_insert(0);
                *count += 1;

                if *count >= config.stale_hysteresis {
                    debug!(
                        order_id = %order.order_id,
                        price = %order.price,
                        current_ask = %current_ask,
                        distance = %distance,
                        "Cancelling truly stale order"
                    );
                    to_cancel.push(order.order_id.clone());
                }
            } else {
                self.stale_counts.remove(&order.order_id);
            }
        }

        self.stale_counts.retain(|id, _| still_open.contains(id));
        to_cancel
    }

    pub fn clear_market(&mut self, market_id: &str) {
        self.last_prices.remove(market_id);
        self.last_inventory.remove(market_id);
        self.last_reladder_ms.remove(market_id);
    }
}

// ============================================================================
// Ladder Order & Plan
// ============================================================================

#[derive(Debug, Clone)]
pub struct LadderOrder {
    pub side: MarketSide,
    pub price: Decimal,
    pub size: Decimal,
    pub is_aggressive: bool,
    pub is_recovery_hunt: bool, // Placed to find cheap recovery pair
}

#[derive(Debug, Default)]
pub struct LadderPlan {
    pub orders: Vec<LadderOrder>,
    pub cancellations: Vec<String>, // ONLY truly stale orders
}

// ============================================================================
// Ladder Engine
// ============================================================================

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

    /// Main entry point
    pub fn compute_ladder(
        &self,
        up_ask: Decimal,
        down_ask: Decimal,
        position: &MarketPosition,
        pending_up: Decimal,
        pending_down: Decimal,
        open_orders: &[OpenOrderInfo],
        cost_tracker: &CostTracker,
        phase: MarketPhase,
        reason: ReladderReason,
    ) -> LadderPlan {
        let mut plan = LadderPlan::default();

        let imbalance = position.up_shares - position.down_shares;
        let abs_imbalance = imbalance.abs();

        trace!(
            up_shares = %position.up_shares,
            down_shares = %position.down_shares,
            imbalance = %imbalance,
            phase = ?phase,
            reason = ?reason,
            avg_cost = ?cost_tracker.avg_pair_cost(),
            "Ladder computation"
        );

        // =================================================================
        // Step 1: NO cancellations here. Only stale_cancellations() does that.
        // =================================================================

        // =================================================================
        // Step 2: Check if we need aggressive rebalance
        // =================================================================
        let needs_rebalance =
            self.should_aggressively_rebalance(abs_imbalance, phase, cost_tracker);

        if needs_rebalance {
            if let Some(rebalance_order) =
                self.compute_rebalance_order(imbalance, up_ask, down_ask, cost_tracker, phase)
            {
                info!(
                    side = ?rebalance_order.side,
                    price = %rebalance_order.price,
                    size = %rebalance_order.size,
                    phase = ?phase,
                    "Aggressive rebalance"
                );
                plan.orders.push(rebalance_order);

                // After rebalance, hunt for cheap recovery pair
                if let Some(recovery_orders) =
                    self.compute_recovery_orders(imbalance, up_ask, down_ask, cost_tracker)
                {
                    plan.orders.extend(recovery_orders);
                }

                return plan;
            }
        }

        // =================================================================
        // Step 3: Normal ladder - ADD orders, don't replace
        // =================================================================
        let total_up = position.up_shares + pending_up;
        let total_down = position.down_shares + pending_down;

        let up_room = (self.config.target_per_side - total_up).max(Decimal::ZERO);
        let down_room = (self.config.target_per_side - total_down).max(Decimal::ZERO);

        // Only add if there's room
        if up_room >= self.config.min_order_size {
            plan.orders.extend(self.generate_side_ladder(
                MarketSide::Up,
                up_ask,
                up_room,
                self.config.target_avg_pair_cost - down_ask, // max price
            ));
        }

        if down_room >= self.config.min_order_size {
            plan.orders.extend(self.generate_side_ladder(
                MarketSide::Down,
                down_ask,
                down_room,
                self.config.target_avg_pair_cost - up_ask, // max price
            ));
        }

        plan
    }

    fn should_aggressively_rebalance(
        &self,
        abs_imbalance: Decimal,
        phase: MarketPhase,
        cost_tracker: &CostTracker,
    ) -> bool {
        match phase {
            MarketPhase::Normal => {
                // Very patient - only rebalance if extremely imbalanced
                abs_imbalance >= self.config.urgent_imbalance_threshold
            }
            MarketPhase::LateGame => {
                // More aggressive - lower threshold
                abs_imbalance >= self.config.late_game_imbalance_threshold
            }
            MarketPhase::Final => {
                // Only rebalance if we're currently profitable on average
                let avg = cost_tracker.avg_pair_cost().unwrap_or(dec!(1.0));
                abs_imbalance >= self.config.min_order_size && avg < dec!(1.0)
            }
        }
    }

    fn compute_rebalance_order(
        &self,
        imbalance: Decimal,
        up_ask: Decimal,
        down_ask: Decimal,
        cost_tracker: &CostTracker,
        phase: MarketPhase,
    ) -> Option<LadderOrder> {
        if imbalance.abs() < self.config.min_order_size {
            return None;
        }

        let (needed_side, ask) = if imbalance > Decimal::ZERO {
            (MarketSide::Down, down_ask)
        } else {
            (MarketSide::Up, up_ask)
        };

        // Price we'd pay (just below ask)
        let rebalance_price = ask - dec!(0.01);

        // What would our average be after this rebalance?
        let projected_avg = cost_tracker.projected_avg_if_rebalance(rebalance_price, needed_side);

        // How much premium over target are we paying?
        let premium = projected_avg - self.config.target_avg_pair_cost;

        // In late game, accept higher premium
        let max_premium = match phase {
            MarketPhase::Normal => self.config.max_rebalance_premium,
            MarketPhase::LateGame => self.config.max_rebalance_premium + dec!(0.02),
            MarketPhase::Final => dec!(0.10), // Accept almost anything to close out
        };

        if premium <= max_premium {
            Some(LadderOrder {
                side: needed_side,
                price: rebalance_price,
                size: imbalance
                    .abs()
                    .min(self.config.base_size_per_level * dec!(2)),
                is_aggressive: true,
                is_recovery_hunt: false,
            })
        } else {
            debug!(
                projected_avg = %projected_avg,
                premium = %premium,
                max_premium = %max_premium,
                "Rebalance would exceed acceptable premium"
            );
            None
        }
    }

    fn compute_recovery_orders(
        &self,
        imbalance: Decimal,
        up_ask: Decimal,
        down_ask: Decimal,
        cost_tracker: &CostTracker,
    ) -> Option<Vec<LadderOrder>> {
        // After a bad rebalance, place aggressive bids for a cheap recovery pair
        let required_pair_cost =
            cost_tracker.required_recovery_pair_cost(self.config.target_avg_pair_cost);

        // If we need a pair at $0.94 to recover, bid aggressively
        if required_pair_cost >= dec!(0.90) {
            let target_per_side = required_pair_cost / dec!(2);

            let mut orders = Vec::new();

            // Bid more aggressively than normal ladder
            let up_recovery_price = target_per_side.min(up_ask - dec!(0.02));
            let down_recovery_price = target_per_side.min(down_ask - dec!(0.02));

            if up_recovery_price >= dec!(0.01) {
                orders.push(LadderOrder {
                    side: MarketSide::Up,
                    price: up_recovery_price,
                    size: self.config.base_size_per_level,
                    is_aggressive: false,
                    is_recovery_hunt: true,
                });
            }

            if down_recovery_price >= dec!(0.01) {
                orders.push(LadderOrder {
                    side: MarketSide::Down,
                    price: down_recovery_price,
                    size: self.config.base_size_per_level,
                    is_aggressive: false,
                    is_recovery_hunt: true,
                });
            }

            info!(
                required_pair_cost = %required_pair_cost,
                up_price = %up_recovery_price,
                down_price = %down_recovery_price,
                "Placing recovery hunt orders"
            );

            return Some(orders);
        }

        None
    }

    fn generate_side_ladder(
        &self,
        side: MarketSide,
        ask: Decimal,
        room: Decimal,
        max_price: Decimal,
    ) -> Vec<LadderOrder> {
        let mut orders = Vec::new();
        let mut remaining = room;

        let top_price = (ask - self.config.top_offset)
            .min(max_price)
            .max(dec!(0.01));

        for level in 0..self.config.levels {
            let price =
                (top_price - self.config.spacing * Decimal::from(level as u32)).max(dec!(0.01));

            if price >= ask || price > max_price {
                continue;
            }

            let size = self.config.base_size_per_level.min(remaining);
            if size < self.config.min_order_size {
                break;
            }

            orders.push(LadderOrder {
                side,
                price,
                size,
                is_aggressive: false,
                is_recovery_hunt: false,
            });

            remaining -= size;
            if remaining < self.config.min_order_size {
                break;
            }
        }

        orders
    }
}
