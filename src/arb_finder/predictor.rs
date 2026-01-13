// arb_finder/predictor.rs
//
// Combines signals into predictions about upcoming arb opportunities.
// Now uses cross-product scanning of full order book depth.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Instant;
use tracing::{debug, info};

use super::config::ArbFinderConfig;
use super::market_state::MarketState;
use super::signals::{Signal, SignalDetector, SignalType};

/// A prediction about an upcoming arb opportunity
#[derive(Debug, Clone)]
pub struct ArbPrediction {
    pub market_id: String,
    pub confidence: Decimal,
    pub signals: Vec<Signal>,
    pub recommended_action: RecommendedAction,
    pub up_target_price: Option<Decimal>,
    pub down_target_price: Option<Decimal>,
    /// The size we can execute at these prices
    pub executable_size: Option<Decimal>,
    /// Profit per share if we execute
    pub profit_per_share: Option<Decimal>,
    pub timestamp: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecommendedAction {
    /// Do nothing, confidence too low
    Wait,
    /// Place limit orders at favorable prices (pre-position)
    PrePosition,
    /// Execute immediately with IOC/market orders
    ExecuteNow,
}

/// Combines signals into predictions
pub struct ArbPredictor {
    config: ArbFinderConfig,
    signal_detector: SignalDetector,
}

impl ArbPredictor {
    pub fn new(config: ArbFinderConfig) -> Self {
        let signal_detector = SignalDetector::new(config.clone());
        Self {
            config,
            signal_detector,
        }
    }

    /// Analyze market state and generate prediction
    /// Now uses cross-product scanning of ALL order book levels
    pub fn predict(&self, state: &MarketState) -> Option<ArbPrediction> {
        // CRITICAL: Scan ALL price levels for arb opportunities, not just top-of-book
        let opportunities = state.find_arb_opportunities(self.config.arb_threshold);
        
        if !opportunities.is_empty() {
            // Found direct arb opportunity in the book!
            let best = &opportunities[0];
            
            info!(
                market_id = %state.market_id,
                up_price = %best.up_price,
                down_price = %best.down_price,
                combined = %best.combined,
                max_size = %best.max_size,
                profit_per_share = %best.profit_per_share,
                total_opportunities = opportunities.len(),
                "🎯 ARB OPPORTUNITY FOUND in order book!"
            );
            
            return Some(ArbPrediction {
                market_id: state.market_id.clone(),
                confidence: dec!(1.0),
                signals: vec![],
                recommended_action: RecommendedAction::ExecuteNow,
                up_target_price: Some(best.up_price),
                down_target_price: Some(best.down_price),
                executable_size: Some(best.max_size),
                profit_per_share: Some(best.profit_per_share),
                timestamp: Instant::now(),
            });
        }
        
        // Log the current best combined price for debugging
        if let Some(min_combined) = state.min_combined() {
            let distance_to_arb = min_combined - self.config.arb_threshold;
            if distance_to_arb < dec!(0.05) {
                debug!(
                    market_id = %state.market_id,
                    min_combined = %min_combined,
                    threshold = %self.config.arb_threshold,
                    distance = %distance_to_arb,
                    "Getting close to arb threshold"
                );
            }
        }

        // Detect signals for predictive positioning
        let signals = self.signal_detector.detect_signals(state);

        if signals.is_empty() {
            return None;
        }

        // Calculate confidence from signals
        let confidence = self.calculate_confidence(&signals);

        // Determine recommended action based on confidence
        let recommended_action = if confidence >= self.config.aggressive_confidence {
            RecommendedAction::ExecuteNow
        } else if confidence >= self.config.pre_position_confidence {
            RecommendedAction::PrePosition
        } else {
            RecommendedAction::Wait
        };

        // Calculate target prices for pre-positioning
        let (up_target, down_target) = self.calculate_target_prices(state, &signals);

        // Only return prediction if action is recommended
        if recommended_action == RecommendedAction::Wait {
            return None;
        }

        // Log at debug level - caller will log at info when actually executing
        // This reduces spam from predictions that hit cooldown
        debug!(
            market_id = %state.market_id,
            confidence = %confidence.round_dp(4),
            action = ?recommended_action,
            signal_count = signals.len(),
            "Signal-based prediction"
        );

        Some(ArbPrediction {
            market_id: state.market_id.clone(),
            confidence,
            signals,
            recommended_action,
            up_target_price: up_target,
            down_target_price: down_target,
            executable_size: None,
            profit_per_share: None,
            timestamp: Instant::now(),
        })
    }

    /// Calculate confidence score from signals
    fn calculate_confidence(&self, signals: &[Signal]) -> Decimal {
        let mut weighted_sum = Decimal::ZERO;
        let mut weight_total = Decimal::ZERO;

        for signal in signals {
            let weight = match signal.signal_type {
                SignalType::Sweep => self.config.sweep_weight,
                SignalType::Imbalance => self.config.imbalance_weight,
                SignalType::Velocity => self.config.velocity_weight,
                SignalType::Discrepancy => self.config.discrepancy_weight,
            };

            weighted_sum += signal.strength * weight;
            weight_total += weight;
        }

        if weight_total == Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Normalize and cap at 1.0
        (weighted_sum / weight_total).min(dec!(1.0))
    }

    /// Calculate target prices by scanning ALL book levels for best combination
    /// Not just top-of-book - look for profitable combinations at any depth
    fn calculate_target_prices(
        &self,
        state: &MarketState,
        _signals: &[Signal],
    ) -> (Option<Decimal>, Option<Decimal>) {
        // Scan the FULL book for the best profitable combination
        // This finds opportunities like: Up @ 45¢ (level 3) + Down @ 48¢ (level 2) = 93¢
        let opportunities = state.find_arb_opportunities(self.config.arb_threshold);
        
        if let Some(best) = opportunities.first() {
            // Found a profitable combination in the book!
            return (Some(best.up_price), Some(best.down_price));
        }
        
        // No profitable combination found - try near-threshold prices
        // Look for the minimum combined price in the book
        if let Some(min_combined) = state.min_combined() {
            // If we're close to threshold, use actual best asks
            if min_combined < dec!(1.02) {
                return (state.up.min_ask(), state.down.min_ask());
            }
        }
        
        // Fallback to best asks (top of book)
        (state.up.best_ask(), state.down.best_ask())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_calculation() {
        let config = ArbFinderConfig::default();
        let predictor = ArbPredictor::new(config);

        // Empty signals should give 0 confidence
        let confidence = predictor.calculate_confidence(&[]);
        assert_eq!(confidence, Decimal::ZERO);
    }
}

