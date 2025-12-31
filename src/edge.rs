use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone)]
pub struct EdgeSignal {
    pub asset: String,
    pub market_id: String,
    pub direction: Direction,
    pub strength: EdgeStrength,
    pub price_change_pct: Decimal,
    pub time_elapsed_pct: f64,
    pub pm_up_ask: Decimal,
    pub pm_down_ask: Decimal,
    pub binance_open: Decimal,
    pub binance_current: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,
    Down,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeStrength {
    Strong,   // High confidence edge
    Moderate, // Decent edge
    Weak,     // Marginal edge
    None,     // No edge
}

pub struct EdgeDetector {
    // Minimum price movement (%) to consider a directional bias
    pub min_move_pct: Decimal,
    // Price move threshold for strong signal
    pub strong_move_pct: Decimal,
    // Minimum time elapsed (%) before we trust the direction
    pub min_time_elapsed_pct: f64,
    // Maximum PM price we'd pay for the favored side
    pub max_entry_price: Decimal,
}

impl Default for EdgeDetector {
    fn default() -> Self {
        Self {
            min_move_pct: dec!(0.003),      // 0.3% move minimum
            strong_move_pct: dec!(0.008),    // 0.8% move is strong
            min_time_elapsed_pct: 0.15,      // Wait at least 15% of market duration
            max_entry_price: dec!(0.55),     // Don't pay more than 0.55 for a side
        }
    }
}

impl EdgeDetector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_thresholds(
        min_move_pct: Decimal,
        strong_move_pct: Decimal,
        min_time_elapsed_pct: f64,
        max_entry_price: Decimal,
    ) -> Self {
        Self {
            min_move_pct,
            strong_move_pct,
            min_time_elapsed_pct,
            max_entry_price,
        }
    }

    /// Analyze if there's an edge based on Binance price movement vs PM prices.
    /// 
    /// Returns Some(EdgeSignal) if edge exists, None otherwise.
    pub fn analyze(
        &self,
        asset: &str,
        market_id: &str,
        binance_open: Decimal,
        binance_current: Decimal,
        pm_up_ask: Option<Decimal>,
        pm_down_ask: Option<Decimal>,
        time_elapsed_pct: f64,
    ) -> Option<EdgeSignal> {
        let (up_ask, down_ask) = match (pm_up_ask, pm_down_ask) {
            (Some(u), Some(d)) => (u, d),
            _ => return None,
        };

        if binance_open.is_zero() {
            return None;
        }

        // Calculate price change percentage
        let price_change = binance_current - binance_open;
        let price_change_pct = price_change / binance_open;

        // Determine direction based on price movement
        let direction = if price_change_pct > self.min_move_pct {
            Direction::Up
        } else if price_change_pct < -self.min_move_pct {
            Direction::Down
        } else {
            Direction::Neutral
        };

        // No edge if neutral or too early
        if direction == Direction::Neutral {
            return None;
        }

        if time_elapsed_pct < self.min_time_elapsed_pct {
            return None;
        }

        // Check if PM prices are mispriced relative to direction
        let (favored_ask, strength) = match direction {
            Direction::Up => {
                let strength = self.calculate_strength(price_change_pct, up_ask, time_elapsed_pct);
                (up_ask, strength)
            }
            Direction::Down => {
                let strength = self.calculate_strength(-price_change_pct, down_ask, time_elapsed_pct);
                (down_ask, strength)
            }
            Direction::Neutral => return None,
        };

        // Only return if we have at least weak edge and price is acceptable
        if strength == EdgeStrength::None || favored_ask > self.max_entry_price {
            return None;
        }

        Some(EdgeSignal {
            asset: asset.to_string(),
            market_id: market_id.to_string(),
            direction,
            strength,
            price_change_pct,
            time_elapsed_pct,
            pm_up_ask: up_ask,
            pm_down_ask: down_ask,
            binance_open,
            binance_current,
        })
    }

    fn calculate_strength(
        &self,
        abs_move_pct: Decimal,
        favored_ask: Decimal,
        time_elapsed_pct: f64,
    ) -> EdgeStrength {
        // The edge exists when:
        // 1. Binance has moved significantly in one direction
        // 2. PM price for that direction is still low (market hasn't adjusted)
        
        // Expected fair price given movement (rough heuristic)
        // If price moved 0.5% up with 50% time elapsed, UP should be ~0.60+
        // If price moved 1% up with 75% time elapsed, UP should be ~0.75+
        
        let move_factor = abs_move_pct / self.strong_move_pct;
        let time_factor = Decimal::try_from(time_elapsed_pct).unwrap_or(dec!(0.5));
        
        // Rough expected probability for favored side
        // Base 0.5 + adjustment for move magnitude and time
        let base_expected = dec!(0.50) + (move_factor * dec!(0.15) * time_factor);
        let expected = base_expected.min(dec!(0.85)); // Cap at 85%

        // Edge = expected - ask (how much PM is underpricing)
        let edge = expected - favored_ask;

        if edge >= dec!(0.10) && abs_move_pct >= self.strong_move_pct {
            EdgeStrength::Strong
        } else if edge >= dec!(0.05) {
            EdgeStrength::Moderate
        } else if edge >= dec!(0.02) {
            EdgeStrength::Weak
        } else {
            EdgeStrength::None
        }
    }
}

impl std::fmt::Display for EdgeSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {:?} [{:?}] | move={:.3}% elapsed={:.1}% | UP_ask={} DOWN_ask={} | open={} cur={}",
            self.asset,
            self.direction,
            self.strength,
            self.price_change_pct * dec!(100),
            self.time_elapsed_pct * 100.0,
            self.pm_up_ask,
            self.pm_down_ask,
            self.binance_open,
            self.binance_current,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_detection_up_move() {
        let detector = EdgeDetector::default();
        
        let signal = detector.analyze(
            "bitcoin",
            "market-123",
            dec!(100000),      // open
            dec!(100800),      // current (0.8% up)
            Some(dec!(0.52)),  // PM UP ask still low
            Some(dec!(0.48)),  // PM DOWN ask
            0.50,              // 50% time elapsed
        );

        assert!(signal.is_some());
        let s = signal.unwrap();
        assert_eq!(s.direction, Direction::Up);
        assert!(s.strength != EdgeStrength::None);
    }

    #[test]
    fn test_no_edge_neutral() {
        let detector = EdgeDetector::default();
        
        let signal = detector.analyze(
            "bitcoin",
            "market-123",
            dec!(100000),
            dec!(100010),      // Only 0.01% move
            Some(dec!(0.50)),
            Some(dec!(0.50)),
            0.50,
        );

        assert!(signal.is_none());
    }
}
