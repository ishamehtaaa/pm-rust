// arb_finder/outcome_tracker.rs
//
// Tracks prediction outcomes for active learning.
// After a prediction is made, monitors if an arb actually occurred.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, info};

use super::config::ArbFinderConfig;
use super::data_logger::{DataLoggerHandle, LogEvent};
use super::market_state::MarketStateStore;
use super::predictor::ArbPrediction;

/// Tracks a pending prediction awaiting outcome
#[derive(Debug, Clone)]
struct PendingPrediction {
    prediction: ArbPrediction,
    created_at: Instant,
}

/// Outcome of a prediction
#[derive(Debug, Clone)]
pub struct PredictionOutcome {
    pub market_id: String,
    pub prediction_confidence: Decimal,
    pub arb_occurred: bool,
    pub best_combined_seen: Option<Decimal>,
    pub profit_captured: Option<Decimal>,
    pub time_to_arb_ms: Option<u64>,
    /// How accurate was this prediction? 1.0 = perfect, 0.0 = wrong
    pub accuracy_score: Decimal,
}

/// Tracks predictions and their outcomes for learning
pub struct OutcomeTracker {
    config: ArbFinderConfig,
    state_store: MarketStateStore,
    data_logger: DataLoggerHandle,
    /// Pending predictions: market_id -> prediction
    pending: HashMap<String, PendingPrediction>,
    /// Recent outcomes for online learning (circular buffer)
    recent_outcomes: Vec<PredictionOutcome>,
    max_recent_outcomes: usize,
}

impl OutcomeTracker {
    pub fn new(
        config: ArbFinderConfig,
        state_store: MarketStateStore,
        data_logger: DataLoggerHandle,
    ) -> Self {
        Self {
            config,
            state_store,
            data_logger,
            pending: HashMap::new(),
            recent_outcomes: Vec::new(),
            max_recent_outcomes: 1000,
        }
    }

    /// Register a new prediction to track
    pub fn track_prediction(&mut self, prediction: ArbPrediction) {
        let market_id = prediction.market_id.clone();

        debug!(
            market_id = %market_id,
            confidence = %prediction.confidence,
            "Tracking prediction"
        );

        self.pending.insert(market_id, PendingPrediction {
            prediction,
            created_at: Instant::now(),
        });
    }

    /// Check all pending predictions for outcomes
    /// Call this periodically (e.g., every scan cycle)
    pub fn check_outcomes(&mut self) -> Vec<PredictionOutcome> {
        let mut outcomes_to_store = Vec::new();
        let mut completed = Vec::new();

        // First pass: evaluate predictions (immutable borrow of self.pending)
        for (market_id, pending) in &self.pending {
            let elapsed = pending.created_at.elapsed();
            
            // Get current market state
            let current_combined = self.state_store
                .get_state(market_id)
                .and_then(|s| s.combined_ask());

            // Check if arb occurred
            let arb_occurred = current_combined
                .map(|c| c < self.config.arb_threshold)
                .unwrap_or(false);

            // Check if prediction timed out
            let timed_out = elapsed > self.config.prediction_timeout;

            if arb_occurred || timed_out {
                let outcome = self.evaluate_prediction(pending, current_combined, arb_occurred);
                
                // Log the outcome
                self.log_outcome(&outcome, pending);
                
                outcomes_to_store.push(outcome);
                completed.push(market_id.clone());
            }
        }

        // Remove completed predictions
        for market_id in &completed {
            self.pending.remove(market_id);
        }

        // Store outcomes (now we can mutably borrow self)
        let mut outcomes = Vec::new();
        for outcome in outcomes_to_store {
            self.store_outcome(outcome.clone());
            outcomes.push(outcome);
        }

        outcomes
    }

    /// Evaluate how accurate a prediction was
    fn evaluate_prediction(
        &self,
        pending: &PendingPrediction,
        current_combined: Option<Decimal>,
        arb_occurred: bool,
    ) -> PredictionOutcome {
        let elapsed_ms = pending.created_at.elapsed().as_millis() as u64;
        let confidence = pending.prediction.confidence;

        // Calculate accuracy score
        let accuracy_score = if arb_occurred {
            // Prediction was correct! Score based on how confident we were
            // High confidence + correct = high score
            confidence
        } else {
            // Prediction was wrong
            // If we were very confident and wrong, that's a big miss
            // If we had low confidence and wrong, that's expected
            dec!(1.0) - confidence
        };

        // Calculate potential profit (if arb occurred)
        let profit_captured = if arb_occurred {
            current_combined.map(|c| {
                let profit_per_pair = dec!(1.0) - c;
                profit_per_pair * self.config.arb_order_size
            })
        } else {
            None
        };

        info!(
            market_id = %pending.prediction.market_id,
            arb_occurred = arb_occurred,
            confidence = %confidence,
            accuracy = %accuracy_score,
            elapsed_ms = elapsed_ms,
            "Prediction outcome"
        );

        PredictionOutcome {
            market_id: pending.prediction.market_id.clone(),
            prediction_confidence: confidence,
            arb_occurred,
            best_combined_seen: current_combined,
            profit_captured,
            time_to_arb_ms: if arb_occurred { Some(elapsed_ms) } else { None },
            accuracy_score,
        }
    }

    fn log_outcome(&self, outcome: &PredictionOutcome, pending: &PendingPrediction) {
        let event = LogEvent::PredictionOutcome {
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            market_id: outcome.market_id.clone(),
            prediction_timestamp_ms: pending.created_at.elapsed().as_millis() as u64,
            arb_occurred: outcome.arb_occurred,
            actual_combined_price: outcome.best_combined_seen.map(|d| d.to_string()),
            profit_captured: outcome.profit_captured.map(|d| d.to_string()),
        };
        
        self.data_logger.log_nonblocking(event);
    }

    fn store_outcome(&mut self, outcome: PredictionOutcome) {
        if self.recent_outcomes.len() >= self.max_recent_outcomes {
            self.recent_outcomes.remove(0);
        }
        self.recent_outcomes.push(outcome);
    }

    /// Get recent outcomes for learning
    pub fn recent_outcomes(&self) -> &[PredictionOutcome] {
        &self.recent_outcomes
    }

    /// Calculate current prediction accuracy (for monitoring)
    pub fn current_accuracy(&self) -> Option<Decimal> {
        if self.recent_outcomes.is_empty() {
            return None;
        }

        let sum: Decimal = self.recent_outcomes.iter()
            .map(|o| o.accuracy_score)
            .sum();
        
        Some(sum / Decimal::from(self.recent_outcomes.len()))
    }

    /// Calculate hit rate (what % of predictions resulted in arbs)
    pub fn hit_rate(&self) -> Option<Decimal> {
        if self.recent_outcomes.is_empty() {
            return None;
        }

        let hits = self.recent_outcomes.iter()
            .filter(|o| o.arb_occurred)
            .count();
        
        Some(Decimal::from(hits) / Decimal::from(self.recent_outcomes.len()))
    }

    /// Get statistics about recent predictions by signal type
    pub fn get_learning_stats(&self) -> LearningStats {
        let total = self.recent_outcomes.len();
        let hits = self.recent_outcomes.iter().filter(|o| o.arb_occurred).count();
        
        let avg_accuracy = self.current_accuracy().unwrap_or(dec!(0.5));
        let hit_rate = self.hit_rate().unwrap_or(Decimal::ZERO);
        
        // Average time to arb for successful predictions
        let avg_time_to_arb: Option<u64> = {
            let arb_times: Vec<_> = self.recent_outcomes.iter()
                .filter_map(|o| o.time_to_arb_ms)
                .collect();
            if arb_times.is_empty() {
                None
            } else {
                Some(arb_times.iter().sum::<u64>() / arb_times.len() as u64)
            }
        };

        LearningStats {
            total_predictions: total,
            successful_predictions: hits,
            hit_rate,
            avg_accuracy,
            avg_time_to_arb_ms: avg_time_to_arb,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LearningStats {
    pub total_predictions: usize,
    pub successful_predictions: usize,
    pub hit_rate: Decimal,
    pub avg_accuracy: Decimal,
    pub avg_time_to_arb_ms: Option<u64>,
}
