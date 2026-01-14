// pair_tracker.rs
//
// Tracks paired positions and calculates P&L for market making.
// A "pair" is 1 Up share + 1 Down share, which pays out $1 regardless of outcome.
// Supports multi-round trading where completed pairs are "locked" and new rounds begin.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::info;

use crate::poller::MarketSide;

/// Tracks fills and calculates pair-based P&L per market
#[derive(Debug, Default)]
pub struct PairTracker {
    /// market_id -> MarketPairState
    markets: HashMap<String, MarketPairState>,
}

/// State for a single market including round tracking
#[derive(Debug, Default, Clone)]
pub struct MarketPairState {
    /// Completed rounds - these pairs are "locked in"
    pub completed_rounds: usize,
    pub locked_pairs: Decimal,
    pub locked_profit: Decimal,
    
    /// Current working round
    pub current_round: RoundState,
}

/// State for the current working round
#[derive(Debug, Default, Clone)]
pub struct RoundState {
    /// Up shares acquired this round
    pub up_shares: Decimal,
    /// Cost paid for Up shares this round
    pub up_cost: Decimal,
    /// Down shares acquired this round
    pub down_shares: Decimal,
    /// Cost paid for Down shares this round
    pub down_cost: Decimal,
}

impl RoundState {
    /// Number of complete pairs in this round
    pub fn paired_shares(&self) -> Decimal {
        self.up_shares.min(self.down_shares)
    }

    /// Unpaired Up shares (exposure to Up outcome)
    pub fn unpaired_up(&self) -> Decimal {
        (self.up_shares - self.down_shares).max(Decimal::ZERO)
    }

    /// Unpaired Down shares (exposure to Down outcome)
    pub fn unpaired_down(&self) -> Decimal {
        (self.down_shares - self.up_shares).max(Decimal::ZERO)
    }

    /// Total unpaired exposure (either direction)
    pub fn total_unpaired(&self) -> Decimal {
        self.unpaired_up() + self.unpaired_down()
    }

    /// Total cost basis for this round
    pub fn total_cost(&self) -> Decimal {
        self.up_cost + self.down_cost
    }

    /// Average cost per Up share
    pub fn avg_up_price(&self) -> Decimal {
        if self.up_shares > Decimal::ZERO {
            self.up_cost / self.up_shares
        } else {
            Decimal::ZERO
        }
    }

    /// Average cost per Down share
    pub fn avg_down_price(&self) -> Decimal {
        if self.down_shares > Decimal::ZERO {
            self.down_cost / self.down_shares
        } else {
            Decimal::ZERO
        }
    }

    /// Profit from paired shares in this round
    pub fn round_profit(&self) -> Decimal {
        let pairs = self.paired_shares();
        if pairs == Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Cost of paired shares
        let pair_cost = pairs * (self.avg_up_price() + self.avg_down_price());
        // Pairs pay out $1 each
        pairs - pair_cost
    }

    /// Check if this round is complete (balanced pairs at target size)
    pub fn is_complete(&self, target_per_round: Decimal) -> bool {
        let pairs = self.paired_shares();
        // Round is complete if we have target pairs AND are balanced
        pairs >= target_per_round && self.total_unpaired() < dec!(1)
    }

    /// Maximum price we can pay for the opposite side to break even
    pub fn max_price_for_hedge(&self, side: MarketSide) -> Decimal {
        match side {
            MarketSide::Up => {
                if self.unpaired_up() > Decimal::ZERO {
                    dec!(1.00) - self.avg_up_price()
                } else {
                    dec!(0.99)
                }
            }
            MarketSide::Down => {
                if self.unpaired_down() > Decimal::ZERO {
                    dec!(1.00) - self.avg_down_price()
                } else {
                    dec!(0.99)
                }
            }
        }
    }

    /// Calculate hedge urgency level and which side needs hedging
    /// Returns (side_that_needs_more, urgency_level)
    /// Urgency level: 0.0 = balanced, 1.0 = critical (at max exposure)
    pub fn hedge_urgency(&self, max_unpaired: Decimal) -> HedgeUrgency {
        let unpaired_up = self.unpaired_up();
        let unpaired_down = self.unpaired_down();

        if unpaired_up > unpaired_down && unpaired_up > dec!(1) {
            // We have more Up shares, need to buy Down
            let urgency = if max_unpaired > Decimal::ZERO {
                (unpaired_up / max_unpaired).min(dec!(1))
            } else {
                dec!(1)
            };
            HedgeUrgency {
                needs_side: MarketSide::Down,
                urgency_level: urgency,
                unpaired_shares: unpaired_up,
            }
        } else if unpaired_down > unpaired_up && unpaired_down > dec!(1) {
            // We have more Down shares, need to buy Up
            let urgency = if max_unpaired > Decimal::ZERO {
                (unpaired_down / max_unpaired).min(dec!(1))
            } else {
                dec!(1)
            };
            HedgeUrgency {
                needs_side: MarketSide::Up,
                urgency_level: urgency,
                unpaired_shares: unpaired_down,
            }
        } else {
            // Balanced
            HedgeUrgency {
                needs_side: MarketSide::Up,  // Arbitrary, won't be used
                urgency_level: Decimal::ZERO,
                unpaired_shares: Decimal::ZERO,
            }
        }
    }
}

/// Represents the urgency to hedge an exposed position
#[derive(Debug, Clone)]
pub struct HedgeUrgency {
    /// The side we need to buy to balance
    pub needs_side: MarketSide,
    /// Urgency level from 0.0 (balanced) to 1.0 (critical)
    pub urgency_level: Decimal,
    /// Number of unpaired shares on the opposite side
    pub unpaired_shares: Decimal,
}

impl HedgeUrgency {
    /// Returns true if urgency is significant (> 0.3)
    pub fn is_significant(&self) -> bool {
        self.urgency_level > dec!(0.3)
    }

    /// Returns true if urgency is high (> 0.5)
    pub fn is_high(&self) -> bool {
        self.urgency_level > dec!(0.5)
    }

    /// Returns true if urgency is critical (> 0.8)
    pub fn is_critical(&self) -> bool {
        self.urgency_level > dec!(0.8)
    }
}

impl MarketPairState {
    /// Total pairs across all rounds (locked + current)
    pub fn total_pairs(&self) -> Decimal {
        self.locked_pairs + self.current_round.paired_shares()
    }

    /// Total profit across all rounds
    pub fn total_profit(&self) -> Decimal {
        self.locked_profit + self.current_round.round_profit()
    }

    /// Check and complete round if ready
    pub fn maybe_complete_round(&mut self, target_per_round: Decimal, max_rounds: usize) -> bool {
        if self.completed_rounds >= max_rounds {
            return false; // Already at max rounds
        }

        if self.current_round.is_complete(target_per_round) {
            // Lock in this round's profit
            let pairs = self.current_round.paired_shares();
            let profit = self.current_round.round_profit();
            
            info!(
                round = self.completed_rounds + 1,
                pairs = %pairs,
                profit = %format!("${:.4}", profit),
                "🎯 Round complete! Locking profit."
            );

            self.locked_pairs += pairs;
            self.locked_profit += profit;
            self.completed_rounds += 1;
            
            // Reset current round (keep any excess shares)
            let excess_up = self.current_round.up_shares - pairs;
            let excess_down = self.current_round.down_shares - pairs;
            
            // Proportional cost for excess shares
            let excess_up_cost = if self.current_round.up_shares > Decimal::ZERO {
                excess_up * self.current_round.avg_up_price()
            } else {
                Decimal::ZERO
            };
            let excess_down_cost = if self.current_round.down_shares > Decimal::ZERO {
                excess_down * self.current_round.avg_down_price()
            } else {
                Decimal::ZERO
            };

            self.current_round = RoundState {
                up_shares: excess_up,
                up_cost: excess_up_cost,
                down_shares: excess_down,
                down_cost: excess_down_cost,
            };

            return true;
        }

        false
    }

    /// Can we start new orders? (not at max rounds, not too much exposure)
    pub fn can_place_orders(&self, max_rounds: usize, max_unpaired: Decimal) -> bool {
        if self.completed_rounds >= max_rounds {
            return false;
        }
        
        self.current_round.total_unpaired() < max_unpaired
    }

    /// How many more shares can we add to current round per side?
    pub fn room_for_side(&self, side: MarketSide, target_per_round: Decimal, max_unpaired: Decimal) -> Decimal {
        let current = match side {
            MarketSide::Up => self.current_round.up_shares,
            MarketSide::Down => self.current_round.down_shares,
        };
        
        // Don't exceed target for the round
        let room_to_target = (target_per_round - current).max(Decimal::ZERO);
        
        // Don't create too much unpaired exposure
        let opposite = match side {
            MarketSide::Up => self.current_round.down_shares,
            MarketSide::Down => self.current_round.up_shares,
        };
        
        // If we add more, how much unpaired would we have?
        // Current unpaired on this side + new = current - opposite + new
        // We want: current - opposite + new <= max_unpaired
        // So: new <= max_unpaired - (current - opposite) = max_unpaired + opposite - current
        let room_for_exposure = (max_unpaired + opposite - current).max(Decimal::ZERO);
        
        room_to_target.min(room_for_exposure)
    }
}

impl PairTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fill and check for round completion
    pub fn record_fill(
        &mut self,
        market_id: &str,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
        target_per_round: Decimal,
        max_rounds: usize,
    ) {
        let state = self.markets.entry(market_id.to_string()).or_default();
        let cost = size * price;

        match side {
            MarketSide::Up => {
                state.current_round.up_shares += size;
                state.current_round.up_cost += cost;
            }
            MarketSide::Down => {
                state.current_round.down_shares += size;
                state.current_round.down_cost += cost;
            }
        }

        // Check if round completed
        state.maybe_complete_round(target_per_round, max_rounds);

        // Log the updated state
        self.log_state(market_id);
    }

    /// Get state for a market
    pub fn get_state(&self, market_id: &str) -> Option<&MarketPairState> {
        self.markets.get(market_id)
    }

    /// Check if we can place more orders for this market
    pub fn can_place_orders(&self, market_id: &str, max_rounds: usize, max_unpaired: Decimal) -> bool {
        match self.markets.get(market_id) {
            Some(state) => state.can_place_orders(max_rounds, max_unpaired),
            None => true, // No state yet, can definitely place
        }
    }

    /// Get room for new orders on a side
    pub fn room_for_side(
        &self,
        market_id: &str,
        side: MarketSide,
        target_per_round: Decimal,
        max_unpaired: Decimal,
    ) -> Decimal {
        match self.markets.get(market_id) {
            Some(state) => state.room_for_side(side, target_per_round, max_unpaired),
            None => target_per_round, // No state, full room
        }
    }

    /// Get hedge urgency for a market
    pub fn hedge_urgency(&self, market_id: &str, max_unpaired: Decimal) -> Option<HedgeUrgency> {
        self.markets
            .get(market_id)
            .map(|state| state.current_round.hedge_urgency(max_unpaired))
    }

    /// Get completed rounds for a market
    pub fn completed_rounds(&self, market_id: &str) -> usize {
        self.markets.get(market_id).map(|s| s.completed_rounds).unwrap_or(0)
    }

    /// Log current state for a market
    fn log_state(&self, market_id: &str) {
        if let Some(state) = self.markets.get(market_id) {
            let round = &state.current_round;
            let paired = round.paired_shares();
            let unpaired_up = round.unpaired_up();
            let unpaired_down = round.unpaired_down();

            info!(
                market_id = %market_id,
                completed_rounds = state.completed_rounds,
                locked_profit = %format!("${:.4}", state.locked_profit),
                current_up = %round.up_shares,
                current_down = %round.down_shares,
                paired = %paired,
                unpaired_up = %unpaired_up,
                unpaired_down = %unpaired_down,
                round_profit = %format!("${:.4}", round.round_profit()),
                total_profit = %format!("${:.4}", state.total_profit()),
                "📊 Pair status"
            );

            // Warn if significantly unbalanced
            if unpaired_up > dec!(5) {
                info!(
                    market_id = %market_id,
                    unpaired = %unpaired_up,
                    max_down_price = %round.max_price_for_hedge(MarketSide::Up),
                    "⚠️  Need Down shares to complete pairs"
                );
            }
            if unpaired_down > dec!(5) {
                info!(
                    market_id = %market_id,
                    unpaired = %unpaired_down,
                    max_up_price = %round.max_price_for_hedge(MarketSide::Down),
                    "⚠️  Need Up shares to complete pairs"
                );
            }
        }
    }

    /// Clear state for a market (e.g., when it resolves)
    pub fn clear_market(&mut self, market_id: &str) {
        if let Some(state) = self.markets.remove(market_id) {
            info!(
                market_id = %market_id,
                total_rounds = state.completed_rounds,
                total_profit = %format!("${:.4}", state.total_profit()),
                "🏁 Market complete"
            );
        }
    }

    /// Get summary of all markets
    pub fn summary(&self) -> PairSummary {
        let mut total_locked_profit = Decimal::ZERO;
        let mut total_round_profit = Decimal::ZERO;
        let mut total_unpaired_up = Decimal::ZERO;
        let mut total_unpaired_down = Decimal::ZERO;
        let mut total_completed_rounds = 0;

        for state in self.markets.values() {
            total_locked_profit += state.locked_profit;
            total_round_profit += state.current_round.round_profit();
            total_unpaired_up += state.current_round.unpaired_up();
            total_unpaired_down += state.current_round.unpaired_down();
            total_completed_rounds += state.completed_rounds;
        }

        PairSummary {
            markets: self.markets.len(),
            total_completed_rounds,
            total_locked_profit,
            total_round_profit,
            total_profit: total_locked_profit + total_round_profit,
            total_unpaired_up,
            total_unpaired_down,
        }
    }
}

#[derive(Debug)]
pub struct PairSummary {
    pub markets: usize,
    pub total_completed_rounds: usize,
    pub total_locked_profit: Decimal,
    pub total_round_profit: Decimal,
    pub total_profit: Decimal,
    pub total_unpaired_up: Decimal,
    pub total_unpaired_down: Decimal,
}
