// arb_finder/mod.rs
//
// Predictive arbitrage finder system.
// Monitors multiple data sources to detect and predict arb opportunities.

pub mod config;
pub mod data_logger;
pub mod market_state;
pub mod outcome_tracker;
pub mod predictor;
pub mod rest_poller;
pub mod signals;
pub mod trade_poller;
pub mod weight_learner;

use rust_decimal::Decimal;
use std::time::{Duration, Instant};
use tracing::{debug, info};

pub use config::ArbFinderConfig;
pub use data_logger::{DataLogger, DataLoggerHandle, LogEvent};
pub use market_state::{MarketState, MarketStateStore, TradeEvent, TradeSide};
pub use outcome_tracker::{OutcomeTracker, PredictionOutcome, LearningStats};
pub use predictor::{ArbPrediction, ArbPredictor, RecommendedAction};
pub use rest_poller::spawn_rest_poller;
pub use signals::{Signal, SignalDetector, SignalType};
pub use trade_poller::spawn_trade_poller;
pub use weight_learner::{WeightLearner, LearnedWeights};

/// Market info for initializing the arb finder
#[derive(Clone)]
pub struct MarketInfo {
    pub market_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
}

/// Main arb finder that coordinates all components
pub struct ArbFinder {
    config: ArbFinderConfig,
    state_store: MarketStateStore,
    predictor: ArbPredictor,
    data_logger: DataLoggerHandle,
    markets: Vec<MarketInfo>,

    // Active learning components
    outcome_tracker: OutcomeTracker,
    weight_learner: WeightLearner,

    // Background tasks
    _rest_poller: Option<tokio::task::JoinHandle<()>>,
    _trade_poller: Option<tokio::task::JoinHandle<()>>,
    _data_logger_task: Option<tokio::task::JoinHandle<()>>,
    _spread_logger_task: Option<tokio::task::JoinHandle<()>>,
}

impl ArbFinder {
    /// Create a new arb finder
    pub fn new(config: ArbFinderConfig) -> Self {
        let state_store = MarketStateStore::new();
        let predictor = ArbPredictor::new(config.clone());

        // Create data logger
        let (logger, logger_handle) = DataLogger::new(config.clone());
        let logger_task = logger.spawn();

        // Create outcome tracker for learning
        let outcome_tracker = OutcomeTracker::new(
            config.clone(),
            state_store.clone(),
            logger_handle.clone(),
        );

        // Create weight learner with saved weights
        let weight_learner = WeightLearner::new(
            rust_decimal_macros::dec!(0.05), // 5% learning rate
            format!("{}/learned_weights.json", config.data_log_path),
        );

        Self {
            config,
            state_store,
            predictor,
            data_logger: logger_handle,
            markets: Vec::new(),
            outcome_tracker,
            weight_learner,
            _rest_poller: None,
            _trade_poller: None,
            _data_logger_task: Some(logger_task),
            _spread_logger_task: None,
        }
    }

    /// Initialize with markets
    pub fn init_markets(&mut self, markets: Vec<MarketInfo>) {
        // Initialize state store for each market
        for market in &markets {
            self.state_store.init_market(
                market.market_id.clone(),
                market.up_token_id.clone(),
                market.down_token_id.clone(),
            );
        }

        self.markets = markets.clone();

        // Start REST poller
        let rest_markets: Vec<_> = markets
            .iter()
            .map(|m| {
                (
                    m.market_id.clone(),
                    m.up_token_id.clone(),
                    m.down_token_id.clone(),
                )
            })
            .collect();

        self._rest_poller = Some(spawn_rest_poller(
            self.config.clone(),
            self.state_store.clone(),
            rest_markets,
        ));

        // Start spread logger (logs combined prices periodically)
        let spread_logger = SpreadLogger::new(
            self.state_store.clone(),
            self.data_logger.clone(),
            Duration::from_secs(1),
        );
        self._spread_logger_task = Some(spread_logger.spawn());

        info!(market_count = markets.len(), "ArbFinder initialized");
    }

    /// Get the state store for external updates
    pub fn state_store(&self) -> &MarketStateStore {
        &self.state_store
    }

    /// Get the data logger handle for external logging
    pub fn data_logger(&self) -> &DataLoggerHandle {
        &self.data_logger
    }

    /// Set the trade poller handle (started externally since it needs client access)
    pub fn set_trade_poller(&mut self, handle: tokio::task::JoinHandle<()>) {
        if let Some(old) = self._trade_poller.take() {
            old.abort();
        }
        self._trade_poller = Some(handle);
    }

    /// Get the config
    pub fn config(&self) -> &ArbFinderConfig {
        &self.config
    }

    /// Update price from WebSocket
    pub fn update_ws_price(&self, token_id: &str, bid: Decimal, ask: Decimal) {
        self.state_store.update_ws_price(token_id, bid, ask);

        // Find market for this token and log
        for market in &self.markets {
            if market.up_token_id == token_id || market.down_token_id == token_id {
                let event =
                    LogEvent::price_update(&market.market_id, token_id, "ws", Some(bid), Some(ask));
                self.data_logger.log_nonblocking(event);
                break;
            }
        }
    }

    /// Record a trade
    pub fn record_trade(&self, token_id: &str, side: TradeSide, price: Decimal, size: Decimal) {
        let trade = TradeEvent {
            timestamp: Instant::now(),
            token_id: token_id.to_string(),
            side,
            price,
            size,
        };

        self.state_store.record_trade(token_id, trade);

        // Log trade
        for market in &self.markets {
            if market.up_token_id == token_id || market.down_token_id == token_id {
                let side_str = match side {
                    TradeSide::Buy => "buy",
                    TradeSide::Sell => "sell",
                };
                let event = LogEvent::trade(&market.market_id, token_id, side_str, price, size);
                self.data_logger.log_nonblocking(event);
                break;
            }
        }
    }

    /// Scan all markets for predictions
    /// Now also tracks outcomes and learns from them
    pub fn scan(&mut self) -> Vec<ArbPrediction> {
        // First, check outcomes of previous predictions and learn
        let outcomes = self.outcome_tracker.check_outcomes();
        for outcome in &outcomes {
            // Get the signals that were present in this prediction
            // For now, we approximate - in production we'd store signals with predictions
            let signals_present: Vec<SignalType> = vec![]; // TODO: Store signals with predictions
            self.weight_learner.learn_from_outcome(outcome, &signals_present);
        }

        let mut predictions = Vec::new();

        for state in self.state_store.all_states() {
            // Use learned weights in predictor
            let learned_weights = self.weight_learner.weights();
            if let Some(prediction) = self.predictor.predict_with_weights(&state, &learned_weights.read()) {
                // Log signals
                for signal in &prediction.signals {
                    let event = LogEvent::signal(&state.market_id, signal);
                    self.data_logger.log_nonblocking(event);
                }

                // Log prediction
                let event = LogEvent::prediction(&prediction);
                self.data_logger.log_nonblocking(event);

                // Track this prediction for outcome evaluation
                self.outcome_tracker.track_prediction(prediction.clone());

                predictions.push(prediction);
            }
        }

        predictions
    }

    /// Get prediction for a specific market
    pub fn predict_market(&self, market_id: &str) -> Option<ArbPrediction> {
        let state = self.state_store.get_state(market_id)?;
        let prediction = self.predictor.predict(&state)?;

        // Log signals and prediction
        for signal in &prediction.signals {
            let event = LogEvent::signal(market_id, signal);
            self.data_logger.log_nonblocking(event);
        }
        let event = LogEvent::prediction(&prediction);
        self.data_logger.log_nonblocking(event);

        Some(prediction)
    }

    /// Check if there's a direct arb opportunity (combined < threshold)
    pub fn check_direct_arb(&self, market_id: &str) -> Option<(Decimal, Decimal, Decimal)> {
        let state = self.state_store.get_state(market_id)?;
        let combined = state.combined_ask()?;

        if combined < self.config.arb_threshold {
            let up_ask = state.up.best_ask()?;
            let down_ask = state.down.best_ask()?;
            Some((up_ask, down_ask, combined))
        } else {
            None
        }
    }

    /// Get predicted prices for a market, accounting for execution delay
    /// Returns (up_price, down_price) adjusted for expected latency
    pub fn predict_prices(&self, market_id: &str, delay_ms: u64) -> Option<(Decimal, Decimal)> {
        let state = self.state_store.get_state(market_id)?;
        state.predict_prices_in_ms(delay_ms)
    }

    /// Check if arb will still be profitable after execution delay
    /// Returns Some((up_price, down_price, combined)) if still profitable
    pub fn check_arb_with_prediction(&self, market_id: &str, delay_ms: u64) -> Option<(Decimal, Decimal, Decimal)> {
        let state = self.state_store.get_state(market_id)?;
        
        // Get predicted prices
        let predicted_combined = state.predict_combined_ask_in_ms(delay_ms)?;
        let (up_predicted, down_predicted) = state.predict_prices_in_ms(delay_ms)?;
        
        // Check if still profitable after delay
        if predicted_combined < self.config.arb_threshold {
            Some((up_predicted, down_predicted, predicted_combined))
        } else {
            // Use aggressive pricing as fallback
            let up_aggressive = (state.up.best_ask()? + rust_decimal_macros::dec!(0.01)).round_dp(2);
            let down_aggressive = (state.down.best_ask()? + rust_decimal_macros::dec!(0.01)).round_dp(2);
            Some((up_aggressive, down_aggressive, up_aggressive + down_aggressive))
        }
    }

    /// Scan for ALL arb opportunities across full book depth
    /// Returns the list of opportunities found (logging is minimal for speed)
    pub fn scan_cross_product_arbs(&self, market_id: &str) -> Vec<market_state::ArbOpportunity> {
        let Some(state) = self.state_store.get_state(market_id) else {
            return Vec::new();
        };

        let opportunities = state.find_arb_opportunities(self.config.arb_threshold);

        // Log to file only (non-blocking), no console spam
        for opp in &opportunities {
            let event = LogEvent::arb_opportunity(market_id, opp);
            self.data_logger.log_nonblocking(event);
        }

        // Only log summary if opportunities found (DEBUG level)
        if !opportunities.is_empty() {
            debug!(
                market_id = %market_id,
                count = opportunities.len(),
                best_profit = %opportunities.first().map(|o| o.profit_per_pair).unwrap_or_default(),
                "Cross-product scan"
            );
        }

        opportunities
    }

    /// Scan all markets for cross-product arb opportunities
    pub fn scan_all_cross_product_arbs(&self) -> Vec<(String, Vec<market_state::ArbOpportunity>)> {
        let mut results = Vec::new();
        
        for market in &self.markets {
            let opps = self.scan_cross_product_arbs(&market.market_id);
            if !opps.is_empty() {
                results.push((market.market_id.clone(), opps));
            }
        }
        
        results
    }

    /// Calculate safe order size for arb based on current position and config limits
    ///
    /// Returns (up_size, down_size) - the safe sizes to order for each side
    /// Takes into account:
    /// - Current position (up_shares, down_shares)
    /// - Max exposure per market
    /// - Base arb order size
    /// - Confidence scaling (optional)
    pub fn calculate_safe_arb_size(
        &self,
        current_up_shares: Decimal,
        current_down_shares: Decimal,
        confidence: Decimal,
    ) -> (Decimal, Decimal) {
        let base_size = self.config.arb_order_size;
        let max_exposure = self.config.max_exposure_per_market.round_dp(2);

        // Calculate current total exposure (max of either side)
        let current_exposure = current_up_shares.max(current_down_shares).round_dp(2);

        // Calculate remaining room before hitting max exposure
        let remaining_room = (max_exposure - current_exposure).max(Decimal::ZERO);

        if remaining_room <= Decimal::ZERO {
            info!(
                current_exposure = %current_exposure,
                max_exposure = %max_exposure,
                "Max exposure reached, no arb orders"
            );
            return (Decimal::ZERO, Decimal::ZERO);
        }

        // Scale base size by confidence (0.5 to 1.0 range -> 50% to 100% of base)
        // Minimum confidence to get any size is the pre_position_confidence threshold
        let confidence_multiplier = if confidence >= self.config.aggressive_confidence {
            Decimal::ONE // Full size for high confidence
        } else {
            // Scale between 0.5 and 1.0 based on confidence
            let min_multiplier = rust_decimal_macros::dec!(0.5);
            let range = Decimal::ONE - min_multiplier;
            let confidence_normalized = (confidence - self.config.pre_position_confidence)
                / (self.config.aggressive_confidence - self.config.pre_position_confidence);
            min_multiplier + (range * confidence_normalized.max(Decimal::ZERO).min(Decimal::ONE))
        };

        let scaled_size = base_size * confidence_multiplier;

        // Cap at remaining room
        let final_size = scaled_size.min(remaining_room);

        // Round to 2 decimal places
        let final_size = final_size.round_dp(2);

        info!(
            base_size = %base_size,
            confidence = %confidence,
            confidence_multiplier = %confidence_multiplier,
            current_exposure = %current_exposure,
            remaining_room = %remaining_room,
            final_size = %final_size,
            "Calculated safe arb size"
        );

        (final_size, final_size)
    }

    /// Get the configured arb order size (without adjustments)
    pub fn base_arb_order_size(&self) -> Decimal {
        self.config.arb_order_size
    }

    /// Get the max exposure per market
    pub fn max_exposure_per_market(&self) -> Decimal {
        self.config.max_exposure_per_market
    }

    /// Get the arb threshold
    pub fn arb_threshold(&self) -> Decimal {
        self.config.arb_threshold
    }

    // ===== Active Learning Methods =====

    /// Get current learning statistics
    pub fn learning_stats(&self) -> LearningStats {
        self.outcome_tracker.get_learning_stats()
    }

    /// Get current prediction accuracy
    pub fn prediction_accuracy(&self) -> Option<Decimal> {
        self.outcome_tracker.current_accuracy()
    }

    /// Get prediction hit rate (% that resulted in actual arbs)
    pub fn prediction_hit_rate(&self) -> Option<Decimal> {
        self.outcome_tracker.hit_rate()
    }

    /// Get current learned weights
    pub fn learned_weights(&self) -> LearnedWeights {
        self.weight_learner.weights().read().clone()
    }

    /// Force save learned weights
    pub fn save_learned_weights(&self) {
        self.weight_learner.save_weights();
    }

    /// Log learning status (for monitoring)
    pub fn log_learning_status(&self) {
        self.weight_learner.log_status();
        
        let stats = self.learning_stats();
        info!(
            total_predictions = stats.total_predictions,
            successful = stats.successful_predictions,
            hit_rate = %stats.hit_rate,
            avg_accuracy = %stats.avg_accuracy,
            avg_time_to_arb_ms = ?stats.avg_time_to_arb_ms,
            "Learning stats"
        );
    }
}

/// Periodically logs spread snapshots
struct SpreadLogger {
    state_store: MarketStateStore,
    data_logger: DataLoggerHandle,
    interval: Duration,
}

impl SpreadLogger {
    fn new(
        state_store: MarketStateStore,
        data_logger: DataLoggerHandle,
        interval: Duration,
    ) -> Self {
        Self {
            state_store,
            data_logger,
            interval,
        }
    }

    fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;

                for state in self.state_store.all_states() {
                    let event = LogEvent::spread_snapshot(
                        &state.market_id,
                        state.up.ws_ask,
                        state.down.ws_ask,
                        state.up.rest_ask,
                        state.down.rest_ask,
                    );
                    let _ = self.data_logger.log(event).await;
                }
            }
        })
    }
}
