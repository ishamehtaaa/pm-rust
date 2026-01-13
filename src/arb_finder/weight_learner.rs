// arb_finder/weight_learner.rs
//
// Online learning for signal weights.
// Adjusts weights based on prediction outcomes using exponential moving average.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use super::outcome_tracker::PredictionOutcome;
use super::signals::SignalType;

/// Learned weights for each signal type
#[derive(Debug, Clone)]
pub struct LearnedWeights {
    pub sweep: Decimal,
    pub imbalance: Decimal,
    pub velocity: Decimal,
    pub discrepancy: Decimal,
}

impl Default for LearnedWeights {
    fn default() -> Self {
        // Start with equal weights
        Self {
            sweep: dec!(0.25),
            imbalance: dec!(0.25),
            velocity: dec!(0.25),
            discrepancy: dec!(0.25),
        }
    }
}

impl LearnedWeights {
    /// Load weights from saved config or use defaults
    pub fn load(path: &str) -> Self {
        // Try to load from file
        if let Ok(contents) = std::fs::read_to_string(path) {
            if let Ok(weights) = serde_json::from_str(&contents) {
                return weights;
            }
        }
        Self::default()
    }

    /// Save weights to file
    pub fn save(&self, path: &str) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)
    }

    pub fn get(&self, signal_type: SignalType) -> Decimal {
        match signal_type {
            SignalType::Sweep => self.sweep,
            SignalType::Imbalance => self.imbalance,
            SignalType::Velocity => self.velocity,
            SignalType::Discrepancy => self.discrepancy,
        }
    }

    pub fn set(&mut self, signal_type: SignalType, value: Decimal) {
        match signal_type {
            SignalType::Sweep => self.sweep = value,
            SignalType::Imbalance => self.imbalance = value,
            SignalType::Velocity => self.velocity = value,
            SignalType::Discrepancy => self.discrepancy = value,
        }
    }

    /// Normalize weights to sum to 1.0
    pub fn normalize(&mut self) {
        let sum = self.sweep + self.imbalance + self.velocity + self.discrepancy;
        if sum > Decimal::ZERO {
            self.sweep /= sum;
            self.imbalance /= sum;
            self.velocity /= sum;
            self.discrepancy /= sum;
        }
    }
}

// Make LearnedWeights serializable
impl serde::Serialize for LearnedWeights {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("LearnedWeights", 4)?;
        state.serialize_field("sweep", &self.sweep.to_string())?;
        state.serialize_field("imbalance", &self.imbalance.to_string())?;
        state.serialize_field("velocity", &self.velocity.to_string())?;
        state.serialize_field("discrepancy", &self.discrepancy.to_string())?;
        state.end()
    }
}

impl<'de> serde::Deserialize<'de> for LearnedWeights {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct WeightsHelper {
            sweep: String,
            imbalance: String,
            velocity: String,
            discrepancy: String,
        }
        
        let helper = WeightsHelper::deserialize(deserializer)?;
        Ok(LearnedWeights {
            sweep: helper.sweep.parse().unwrap_or(dec!(0.25)),
            imbalance: helper.imbalance.parse().unwrap_or(dec!(0.25)),
            velocity: helper.velocity.parse().unwrap_or(dec!(0.25)),
            discrepancy: helper.discrepancy.parse().unwrap_or(dec!(0.25)),
        })
    }
}

/// Signal effectiveness tracking
#[derive(Debug, Clone, Default)]
struct SignalStats {
    /// Times this signal was present in a prediction
    appearances: u64,
    /// Times the prediction was correct when signal was present
    successes: u64,
    /// Running average of accuracy when signal was present
    avg_accuracy: Decimal,
}

/// Online weight learner using EMA updates
pub struct WeightLearner {
    /// Current learned weights
    weights: Arc<RwLock<LearnedWeights>>,
    /// Learning rate for EMA updates (0.0 to 1.0)
    /// Higher = faster adaptation, more volatile
    /// Lower = slower adaptation, more stable
    learning_rate: Decimal,
    /// Per-signal statistics
    signal_stats: HashMap<SignalType, SignalStats>,
    /// Path to save weights
    save_path: String,
    /// How often to save (in updates)
    save_interval: u64,
    /// Update counter
    update_count: u64,
}

impl WeightLearner {
    pub fn new(learning_rate: Decimal, save_path: String) -> Self {
        let weights = LearnedWeights::load(&save_path);
        info!(
            sweep = %weights.sweep,
            imbalance = %weights.imbalance,
            velocity = %weights.velocity,
            discrepancy = %weights.discrepancy,
            "Loaded learned weights"
        );

        Self {
            weights: Arc::new(RwLock::new(weights)),
            learning_rate,
            signal_stats: HashMap::new(),
            save_path,
            save_interval: 100,
            update_count: 0,
        }
    }

    /// Get current weights (thread-safe)
    pub fn weights(&self) -> Arc<RwLock<LearnedWeights>> {
        self.weights.clone()
    }

    /// Get current weight for a signal type
    pub fn get_weight(&self, signal_type: SignalType) -> Decimal {
        self.weights.read().get(signal_type)
    }

    /// Learn from an outcome
    /// Call this after each prediction outcome is determined
    pub fn learn_from_outcome(
        &mut self,
        outcome: &PredictionOutcome,
        signals_present: &[SignalType],
    ) {
        if signals_present.is_empty() {
            return;
        }

        let success = outcome.arb_occurred;
        let accuracy = outcome.accuracy_score;

        // Update stats for each signal that was present
        for signal_type in signals_present {
            let stats = self.signal_stats.entry(*signal_type).or_default();
            stats.appearances += 1;
            if success {
                stats.successes += 1;
            }
            // EMA update for accuracy
            stats.avg_accuracy = stats.avg_accuracy * (dec!(1.0) - self.learning_rate)
                + accuracy * self.learning_rate;
        }

        // Update weights based on signal performance
        self.update_weights(signals_present, success);

        self.update_count += 1;
        
        // Periodically save weights
        if self.update_count % self.save_interval == 0 {
            self.save_weights();
        }
    }

    /// Update weights using EMA based on which signals led to success/failure
    fn update_weights(&mut self, signals_present: &[SignalType], success: bool) {
        let mut weights = self.weights.write();
        
        // Calculate adjustment
        // If successful: increase weight of present signals
        // If failed: decrease weight of present signals
        let adjustment = if success {
            self.learning_rate
        } else {
            -self.learning_rate * dec!(0.5) // Penalize less harshly for failures
        };

        for signal_type in signals_present {
            let current = weights.get(*signal_type);
            // Apply adjustment with bounds
            let new_weight = (current + adjustment)
                .max(dec!(0.05))  // Minimum weight
                .min(dec!(0.6));  // Maximum weight
            weights.set(*signal_type, new_weight);
        }

        // Normalize so weights sum to 1.0
        weights.normalize();

        debug!(
            sweep = %weights.sweep,
            imbalance = %weights.imbalance,
            velocity = %weights.velocity,
            discrepancy = %weights.discrepancy,
            "Updated weights"
        );
    }

    /// Save weights to file
    pub fn save_weights(&self) {
        let weights = self.weights.read();
        if let Err(e) = weights.save(&self.save_path) {
            debug!(error = %e, "Failed to save weights");
        } else {
            info!(path = %self.save_path, "Saved learned weights");
        }
    }

    /// Get learning statistics
    pub fn get_stats(&self) -> HashMap<SignalType, (u64, u64, Decimal)> {
        self.signal_stats.iter()
            .map(|(k, v)| (*k, (v.appearances, v.successes, v.avg_accuracy)))
            .collect()
    }

    /// Get hit rate for a specific signal type
    pub fn signal_hit_rate(&self, signal_type: SignalType) -> Option<Decimal> {
        self.signal_stats.get(&signal_type).map(|stats| {
            if stats.appearances == 0 {
                Decimal::ZERO
            } else {
                Decimal::from(stats.successes) / Decimal::from(stats.appearances)
            }
        })
    }

    /// Log current learning status
    pub fn log_status(&self) {
        let weights = self.weights.read();
        
        info!(
            update_count = self.update_count,
            sweep_weight = %weights.sweep,
            imbalance_weight = %weights.imbalance,
            velocity_weight = %weights.velocity,
            discrepancy_weight = %weights.discrepancy,
            "Weight learner status"
        );

        for (signal_type, stats) in &self.signal_stats {
            let hit_rate = if stats.appearances > 0 {
                Decimal::from(stats.successes) / Decimal::from(stats.appearances)
            } else {
                Decimal::ZERO
            };
            
            info!(
                signal = ?signal_type,
                appearances = stats.appearances,
                successes = stats.successes,
                hit_rate = %hit_rate,
                avg_accuracy = %stats.avg_accuracy,
                "Signal stats"
            );
        }
    }
}
