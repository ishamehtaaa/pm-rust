// pair_tracker.rs
//
// Tracks paired positions and calculates P&L for market making.
// A "pair" is 1 Up share + 1 Down share, which pays out $1 regardless of outcome.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tracing::info;

use crate::poller::MarketSide;

/// Tracks fills and calculates pair-based P&L per market
#[derive(Debug, Default)]
pub struct PairTracker {
    /// market_id -> PairState
    markets: HashMap<String, PairState>,
}

#[derive(Debug, Default, Clone)]
pub struct PairState {
    /// Total Up shares acquired
    pub up_shares: Decimal,
    /// Total cost paid for Up shares
    pub up_cost: Decimal,
    /// Total Down shares acquired
    pub down_shares: Decimal,
    /// Total cost paid for Down shares
    pub down_cost: Decimal,
}

impl PairState {
    /// Number of complete pairs (min of up and down)
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

    /// Total cost basis
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

    /// Guaranteed profit from paired shares
    /// Each pair pays $1, so profit = pairs - cost_of_pairs
    pub fn locked_profit(&self) -> Decimal {
        let pairs = self.paired_shares();
        if pairs == Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Cost of paired shares = (pairs * avg_up_price) + (pairs * avg_down_price)
        let pair_cost = pairs * (self.avg_up_price() + self.avg_down_price());
        // Pairs pay out $1 each
        pairs - pair_cost
    }

    /// Maximum price we can pay for the opposite side to break even on unpaired shares
    pub fn max_price_for_hedge(&self, side: MarketSide) -> Decimal {
        match side {
            // If we have unpaired Up, what's max we can pay for Down?
            MarketSide::Up => {
                if self.unpaired_up() > Decimal::ZERO {
                    // We paid avg_up_price for Up, need combined < 1.00
                    dec!(1.00) - self.avg_up_price()
                } else {
                    dec!(0.99) // No unpaired, use default max
                }
            }
            // If we have unpaired Down, what's max we can pay for Up?
            MarketSide::Down => {
                if self.unpaired_down() > Decimal::ZERO {
                    dec!(1.00) - self.avg_down_price()
                } else {
                    dec!(0.99)
                }
            }
        }
    }
}

impl PairTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a fill
    pub fn record_fill(
        &mut self,
        market_id: &str,
        side: MarketSide,
        size: Decimal,
        price: Decimal,
    ) {
        let state = self.markets.entry(market_id.to_string()).or_default();
        let cost = size * price;

        match side {
            MarketSide::Up => {
                state.up_shares += size;
                state.up_cost += cost;
            }
            MarketSide::Down => {
                state.down_shares += size;
                state.down_cost += cost;
            }
        }

        // Log the updated state
        self.log_state(market_id);
    }

    /// Get state for a market
    pub fn get_state(&self, market_id: &str) -> Option<&PairState> {
        self.markets.get(market_id)
    }

    /// Log current state for a market
    fn log_state(&self, market_id: &str) {
        if let Some(state) = self.markets.get(market_id) {
            let paired = state.paired_shares();
            let unpaired_up = state.unpaired_up();
            let unpaired_down = state.unpaired_down();
            let locked_profit = state.locked_profit();

            info!(
                market_id = %market_id,
                up_shares = %state.up_shares,
                down_shares = %state.down_shares,
                paired = %paired,
                unpaired_up = %unpaired_up,
                unpaired_down = %unpaired_down,
                total_cost = %format!("${:.2}", state.total_cost()),
                locked_profit = %format!("${:.4}", locked_profit),
                "📊 Pair status"
            );

            // Warn if significantly unbalanced
            if unpaired_up > dec!(5) {
                info!(
                    market_id = %market_id,
                    unpaired = %unpaired_up,
                    max_down_price = %state.max_price_for_hedge(MarketSide::Up),
                    "⚠️  Need Down shares to complete pairs"
                );
            }
            if unpaired_down > dec!(5) {
                info!(
                    market_id = %market_id,
                    unpaired = %unpaired_down,
                    max_up_price = %state.max_price_for_hedge(MarketSide::Down),
                    "⚠️  Need Up shares to complete pairs"
                );
            }
        }
    }

    /// Clear state for a market (e.g., when it resolves)
    pub fn clear_market(&mut self, market_id: &str) {
        self.markets.remove(market_id);
    }

    /// Get summary of all markets
    pub fn summary(&self) -> PairSummary {
        let mut total_locked_profit = Decimal::ZERO;
        let mut total_unpaired_up = Decimal::ZERO;
        let mut total_unpaired_down = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;

        for state in self.markets.values() {
            total_locked_profit += state.locked_profit();
            total_unpaired_up += state.unpaired_up();
            total_unpaired_down += state.unpaired_down();
            total_cost += state.total_cost();
        }

        PairSummary {
            markets: self.markets.len(),
            total_locked_profit,
            total_unpaired_up,
            total_unpaired_down,
            total_cost,
        }
    }
}

#[derive(Debug)]
pub struct PairSummary {
    pub markets: usize,
    pub total_locked_profit: Decimal,
    pub total_unpaired_up: Decimal,
    pub total_unpaired_down: Decimal,
    pub total_cost: Decimal,
}


