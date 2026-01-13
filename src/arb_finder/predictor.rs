// arb_finder/predictor.rs
//
// Combines signals into predictions about upcoming arb opportunities.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Instant;
use tracing::{debug, info};

use super::config::ArbFinderConfig;
use super::market_state::MarketState;
use super::signals::{Signal, SignalDetector, SignalType};
use super::weight_learner::LearnedWeights;

/// A prediction about an upcoming arb opportunity
#[derive(Debug, Clone)]
pub struct ArbPrediction {
    pub market_id: String,
    pub confidence: Decimal,
    pub signals: Vec<Signal>,
    pub recommended_action: RecommendedAction,
    pub up_target_price: Option<Decimal>,
    pub down_target_price: Option<Decimal>,
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
    pub fn predict(&self, state: &MarketState) -> Option<ArbPrediction> {
        // First, check if there's already an arb opportunity
        if let Some(combined) = state.combined_ask() {
            if combined < self.config.arb_threshold {
                // Direct arb opportunity!
                return Some(ArbPrediction {
                    market_id: state.market_id.clone(),
                    confidence: dec!(1.0),
                    signals: vec![],
                    recommended_action: RecommendedAction::ExecuteNow,
                    up_target_price: state.up.best_ask(),
                    down_target_price: state.down.best_ask(),
                    timestamp: Instant::now(),
                });
            }
        }

        // Detect signals
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
            debug!(
                market_id = %state.market_id,
                confidence = %confidence,
                signal_count = signals.len(),
                "Low confidence, no action"
            );
            return None;
        }

        info!(
            market_id = %state.market_id,
            confidence = %confidence,
            action = ?recommended_action,
            signal_count = signals.len(),
            "Arb prediction generated"
        );

        Some(ArbPrediction {
            market_id: state.market_id.clone(),
            confidence,
            signals,
            recommended_action,
            up_target_price: up_target,
            down_target_price: down_target,
            timestamp: Instant::now(),
        })
    }

    /// Calculate confidence score from signals using default config weights
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

    /// Calculate confidence using LEARNED weights
    fn calculate_confidence_with_weights(&self, signals: &[Signal], weights: &LearnedWeights) -> Decimal {
        let mut weighted_sum = Decimal::ZERO;
        let mut weight_total = Decimal::ZERO;

        for signal in signals {
            let weight = weights.get(signal.signal_type);
            weighted_sum += signal.strength * weight;
            weight_total += weight;
        }

        if weight_total == Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Normalize and cap at 1.0
        (weighted_sum / weight_total).min(dec!(1.0))
    }

    /// Predict using learned weights (for active learning)
    pub fn predict_with_weights(&self, state: &MarketState, weights: &LearnedWeights) -> Option<ArbPrediction> {
        // First, check if there's already an arb opportunity
        if let Some(combined) = state.combined_ask() {
            if combined < self.config.arb_threshold {
                // Direct arb opportunity!
                return Some(ArbPrediction {
                    market_id: state.market_id.clone(),
                    confidence: dec!(1.0),
                    signals: vec![],
                    recommended_action: RecommendedAction::ExecuteNow,
                    up_target_price: state.up.best_ask(),
                    down_target_price: state.down.best_ask(),
                    timestamp: Instant::now(),
                });
            }
        }

        // Detect signals
        let signals = self.signal_detector.detect_signals(state);

        if signals.is_empty() {
            return None;
        }

        // Calculate confidence using LEARNED weights
        let confidence = self.calculate_confidence_with_weights(&signals, weights);

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
            debug!(
                market_id = %state.market_id,
                confidence = %confidence,
                signal_count = signals.len(),
                "Low confidence (learned weights), no action"
            );
            return None;
        }

        info!(
            market_id = %state.market_id,
            confidence = %confidence,
            action = ?recommended_action,
            signal_count = signals.len(),
            "Arb prediction generated (learned weights)"
        );

        Some(ArbPrediction {
            market_id: state.market_id.clone(),
            confidence,
            signals,
            recommended_action,
            up_target_price: up_target,
            down_target_price: down_target,
            timestamp: Instant::now(),
        })
    }

    /// Calculate target prices for pre-positioning
    fn calculate_target_prices(
        &self,
        state: &MarketState,
        _signals: &[Signal],
    ) -> (Option<Decimal>, Option<Decimal>) {
        // For pre-positioning, we want to place orders slightly below current ask
        // These should be prices that would result in profitable arb if filled

        let combined = state.combined_ask().unwrap_or(dec!(1.05));
        let _profit_needed = dec!(1.0) - self.config.arb_threshold;

        // Current overage from arb threshold
        let overage = combined - self.config.arb_threshold;

        // We need to shave off the overage + some buffer
        let adjustment_per_side = overage / dec!(2.0) + dec!(0.005);

        let up_target = state.up.best_ask().map(|p| (p - adjustment_per_side).max(dec!(0.01)));
        let down_target = state.down.best_ask().map(|p| (p - adjustment_per_side).max(dec!(0.01)));

        (up_target, down_target)
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

