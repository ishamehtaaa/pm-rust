// aggregate_tracker.rs - Track weighted average costs for aggregate edge calculation

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

use crate::poller::MarketSide;

/// Tracks the aggregate position for a single market.
/// The key metric is combined_avg_cost - if this is < $1, we have edge on paired shares.
#[derive(Debug, Default, Clone)]
pub struct AggregatePosition {
    pub up_shares: Decimal,
    pub up_total_cost: Decimal,
    pub down_shares: Decimal,
    pub down_total_cost: Decimal,
}

impl AggregatePosition {
    /// Weighted average price for Up side
    pub fn up_avg_price(&self) -> Option<Decimal> {
        if self.up_shares > Decimal::ZERO {
            Some(self.up_total_cost / self.up_shares)
        } else {
            None
        }
    }

    /// Weighted average price for Down side
    pub fn down_avg_price(&self) -> Option<Decimal> {
        if self.down_shares > Decimal::ZERO {
            Some(self.down_total_cost / self.down_shares)
        } else {
            None
        }
    }

    /// Combined average cost - THE KEY METRIC
    /// If this is < $1, the paired portion is profitable
    pub fn combined_avg_cost(&self) -> Option<Decimal> {
        match (self.up_avg_price(), self.down_avg_price()) {
            (Some(up), Some(down)) => Some(up + down),
            _ => None,
        }
    }

    /// Current imbalance (positive = more Up shares, negative = more Down)
    pub fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }

    /// Shares that form complete pairs (min of both sides)
    pub fn paired_shares(&self) -> Decimal {
        self.up_shares.min(self.down_shares)
    }

    /// Edge per paired share (1.00 - combined_avg_cost)
    pub fn edge_per_share(&self) -> Option<Decimal> {
        self.combined_avg_cost().map(|c| dec!(1) - c)
    }

    /// Guaranteed profit on paired shares
    pub fn guaranteed_profit(&self) -> Option<Decimal> {
        let edge = self.edge_per_share()?;
        Some(edge * self.paired_shares())
    }

    /// Record a fill
    pub fn record_fill(&mut self, side: MarketSide, shares: Decimal, price: Decimal) {
        let cost = shares * price;
        match side {
            MarketSide::Up => {
                self.up_shares += shares;
                self.up_total_cost += cost;
            }
            MarketSide::Down => {
                self.down_shares += shares;
                self.down_total_cost += cost;
            }
        }
    }

    /// Project what combined cost would be if we added this trade
    pub fn projected_combined_cost(
        &self,
        side: MarketSide,
        shares: Decimal,
        price: Decimal,
    ) -> Option<Decimal> {
        let mut projected = self.clone();
        projected.record_fill(side, shares, price);
        projected.combined_avg_cost()
    }

    /// Would adding this trade keep aggregate cost at or below threshold?
    pub fn would_maintain_edge(
        &self,
        side: MarketSide,
        shares: Decimal,
        price: Decimal,
        max_combined: Decimal,
    ) -> bool {
        match self.projected_combined_cost(side, shares, price) {
            Some(cost) => cost <= max_combined,
            None => true, // First trade on a side, allow it
        }
    }

    /// Which side should we prioritize based on imbalance?
    /// Returns (priority_side, other_side)
    pub fn priority_side(&self, max_imbalance: Decimal) -> (MarketSide, MarketSide) {
        let imbalance = self.imbalance();
        if imbalance > max_imbalance {
            // Too many Up shares, need more Down
            (MarketSide::Down, MarketSide::Up)
        } else if imbalance < -max_imbalance {
            // Too many Down shares, need more Up
            (MarketSide::Up, MarketSide::Down)
        } else {
            // Balanced enough - default to Up first (arbitrary)
            (MarketSide::Up, MarketSide::Down)
        }
    }

    /// How many more shares can we add to a side while staying under target?
    pub fn room_on_side(&self, side: MarketSide, target_per_side: Decimal) -> Decimal {
        let current = match side {
            MarketSide::Up => self.up_shares,
            MarketSide::Down => self.down_shares,
        };
        (target_per_side - current).max(Decimal::ZERO)
    }

    /// Reset position (for market rollover)
    pub fn reset(&mut self) {
        self.up_shares = Decimal::ZERO;
        self.up_total_cost = Decimal::ZERO;
        self.down_shares = Decimal::ZERO;
        self.down_total_cost = Decimal::ZERO;
    }
}

/// Tracks aggregate positions across all markets
#[derive(Debug, Default)]
pub struct AggregateTracker {
    positions: HashMap<String, AggregatePosition>,
}

impl AggregateTracker {
    pub fn get(&self, market_id: &str) -> Option<&AggregatePosition> {
        self.positions.get(market_id)
    }

    pub fn get_mut(&mut self, market_id: &str) -> &mut AggregatePosition {
        self.positions.entry(market_id.to_string()).or_default()
    }

    pub fn record_fill(
        &mut self,
        market_id: &str,
        side: MarketSide,
        shares: Decimal,
        price: Decimal,
    ) {
        self.get_mut(market_id).record_fill(side, shares, price);
    }

    pub fn remove_market(&mut self, market_id: &str) {
        self.positions.remove(market_id);
    }

    /// Initialize from existing fills (e.g., on startup or reconciliation)
    pub fn initialize_from_fills(
        &mut self,
        market_id: &str,
        fills: &[(MarketSide, Decimal, Decimal)], // (side, shares, price)
    ) {
        let pos = self.get_mut(market_id);
        pos.reset();
        for (side, shares, price) in fills {
            pos.record_fill(*side, *shares, *price);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combined_cost() {
        let mut pos = AggregatePosition::default();
        
        // Buy 100 Up @ 0.40
        pos.record_fill(MarketSide::Up, dec!(100), dec!(0.40));
        assert_eq!(pos.up_avg_price(), Some(dec!(0.40)));
        assert_eq!(pos.combined_avg_cost(), None); // No down yet
        
        // Buy 100 Down @ 0.55
        pos.record_fill(MarketSide::Down, dec!(100), dec!(0.55));
        assert_eq!(pos.down_avg_price(), Some(dec!(0.55)));
        assert_eq!(pos.combined_avg_cost(), Some(dec!(0.95))); // 5% edge!
        
        // Edge = 1.00 - 0.95 = 0.05 per share
        assert_eq!(pos.edge_per_share(), Some(dec!(0.05)));
        assert_eq!(pos.guaranteed_profit(), Some(dec!(5))); // 100 * 0.05
    }

    #[test]
    fn test_imbalance_priority() {
        let mut pos = AggregatePosition::default();
        pos.up_shares = dec!(150);
        pos.down_shares = dec!(100);
        
        // Imbalance = +50, with max_imbalance = 30, should prioritize Down
        let (priority, _) = pos.priority_side(dec!(30));
        assert_eq!(priority, MarketSide::Down);
        
        // With max_imbalance = 100, balanced enough
        let (priority, _) = pos.priority_side(dec!(100));
        assert_eq!(priority, MarketSide::Up); // Default
    }

    #[test]
    fn test_projected_cost() {
        let mut pos = AggregatePosition::default();
        pos.record_fill(MarketSide::Up, dec!(100), dec!(0.40));
        pos.record_fill(MarketSide::Down, dec!(100), dec!(0.55));
        
        // Current combined = 0.95
        // If we add 100 Up @ 0.50, new up_avg = (40 + 50) / 200 = 0.45
        // New combined = 0.45 + 0.55 = 1.00
        let projected = pos.projected_combined_cost(MarketSide::Up, dec!(100), dec!(0.50));
        assert_eq!(projected, Some(dec!(1.00)));
        
        // Should not maintain edge with max 0.99
        assert!(!pos.would_maintain_edge(MarketSide::Up, dec!(100), dec!(0.50), dec!(0.99)));
    }
}
