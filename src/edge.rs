use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub asset: String,
    pub market_id: String,
    pub binance_open: Decimal,
    pub binance_current: Decimal,
    pub pm_up_ask: Decimal,
    pub pm_down_ask: Decimal,
    pub elapsed_pct: f64,  // 0.0 to 1.0
}

#[derive(Debug, Clone)]
pub struct EdgeSignal {
    pub asset: String,
    pub market_id: String,
    pub side: Side,
    pub fair_value: Decimal,
    pub market_ask: Decimal,
    pub edge: Decimal,          // fair_value - market_ask
    pub price_move_pct: Decimal,
    pub elapsed_pct: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Up,
    Down,
}

impl std::fmt::Display for EdgeSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {:?} | fair={:.3} ask={:.3} edge={:.3} | move={:+.2}% elapsed={:.0}%",
            self.asset,
            self.side,
            self.fair_value,
            self.market_ask,
            self.edge,
            self.price_move_pct * dec!(100),
            self.elapsed_pct * 100.0
        )
    }
}

pub struct EdgeDetector {
    pub min_edge: Decimal,           // Minimum edge to signal (e.g., 0.05 = 5 cents)
    pub max_entry_price: Decimal,    // Don't buy if ask is above this
    pub min_elapsed_pct: f64,        // Don't trade in first X% of market
}

impl Default for EdgeDetector {
    fn default() -> Self {
        Self {
            min_edge: dec!(0.05),
            max_entry_price: dec!(0.60),
            min_elapsed_pct: 0.20,
        }
    }
}

impl EdgeDetector {
    pub fn analyze(&self, snapshot: &MarketSnapshot) -> Option<EdgeSignal> {
        if snapshot.binance_open.is_zero() {
            return None;
        }

        if snapshot.elapsed_pct < self.min_elapsed_pct {
            return None;
        }

        let move_pct = (snapshot.binance_current - snapshot.binance_open) / snapshot.binance_open;

        // Determine which side is favored
        let (side, market_ask) = if move_pct > Decimal::ZERO {
            (Side::Up, snapshot.pm_up_ask)
        } else if move_pct < Decimal::ZERO {
            (Side::Down, snapshot.pm_down_ask)
        } else {
            return None;
        };

        // Skip if market price already too high
        if market_ask > self.max_entry_price {
            return None;
        }

        // Estimate fair value for the favored side
        let fair_value = estimate_fair_value(move_pct.abs(), snapshot.elapsed_pct);

        let edge = fair_value - market_ask;

        if edge < self.min_edge {
            return None;
        }

        Some(EdgeSignal {
            asset: snapshot.asset.clone(),
            market_id: snapshot.market_id.clone(),
            side,
            fair_value,
            market_ask,
            edge,
            price_move_pct: move_pct,
            elapsed_pct: snapshot.elapsed_pct,
        })
    }
}

/// Estimate fair value for the "in the money" side based on:
/// - How far price has moved from open (move_pct, as absolute value)
/// - How much time has elapsed (elapsed_pct, 0.0 to 1.0)
///
/// Intuition:
/// - Larger moves = higher probability of staying in the money
/// - More time elapsed = less time to revert, higher probability of staying
///
/// This uses a simple model: the probability of reverting decreases as:
/// 1. The move size increases (harder to overcome)
/// 2. Time remaining decreases (less opportunity to revert)
fn estimate_fair_value(move_pct: Decimal, elapsed_pct: f64) -> Decimal {
    // Convert to f64 for math
    let move_pct_f64 = move_pct.to_string().parse::<f64>().unwrap_or(0.0);
    
    // Base probability starts at 0.50 (fair coin)
    // We adjust based on move magnitude and time elapsed
    
    // Move contribution: each 0.1% move adds confidence
    // Scaled by a factor that represents typical 15m volatility (~0.3-0.5% for BTC)
    // A 0.5% move is significant, a 1% move is very significant
    let move_factor = (move_pct_f64 / 0.005).min(2.0); // Cap at 2x contribution
    
    // Time contribution: as time passes, current state is more likely to persist
    // At 50% elapsed, moderate confidence boost
    // At 80% elapsed, high confidence boost
    let time_factor = elapsed_pct.powf(0.5); // sqrt gives diminishing returns
    
    // Combined probability adjustment
    // Base 0.50 + adjustment from move + adjustment from time
    let adjustment = 0.15 * move_factor * time_factor;
    let fair_prob = (0.50 + adjustment).min(0.85); // Cap at 85%
    
    Decimal::try_from(fair_prob).unwrap_or(dec!(0.50))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fair_value_estimation() {
        // Small move, early in market
        let fv = estimate_fair_value(dec!(0.002), 0.30);
        assert!(fv > dec!(0.50) && fv < dec!(0.60));
        
        // Large move, late in market  
        let fv = estimate_fair_value(dec!(0.008), 0.80);
        assert!(fv > dec!(0.70));
        
        // Medium move, middle of market
        let fv = estimate_fair_value(dec!(0.005), 0.50);
        assert!(fv > dec!(0.55) && fv < dec!(0.70));
    }

    #[test]
    fn test_edge_detection() {
        let detector = EdgeDetector::default();
        
        let snapshot = MarketSnapshot {
            asset: "bitcoin".into(),
            market_id: "test".into(),
            binance_open: dec!(100000),
            binance_current: dec!(100800), // +0.8%
            pm_up_ask: dec!(0.52),
            pm_down_ask: dec!(0.50),
            elapsed_pct: 0.60,
        };
        
        let signal = detector.analyze(&snapshot);
        assert!(signal.is_some());
        
        let s = signal.unwrap();
        assert_eq!(s.side, Side::Up);
        assert!(s.edge > dec!(0.05));
    }
}