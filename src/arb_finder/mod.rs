// arb_finder/mod.rs
//
// Predictive arbitrage finder system.
// Monitors multiple data sources to detect and predict arb opportunities.

pub mod config;
pub mod data_logger;
pub mod market_state;
pub mod predictor;
pub mod rest_poller;
pub mod signals;

use rust_decimal::Decimal;
use std::time::{Duration, Instant};
use tracing::{debug, info};

pub use config::ArbFinderConfig;
pub use data_logger::{DataLogger, DataLoggerHandle, LogEvent};
pub use market_state::{ArbOpportunity, MarketState, MarketStateStore, PriceLevel, TradeEvent, TradeSide};
pub use predictor::{ArbPrediction, ArbPredictor, RecommendedAction};
pub use rest_poller::spawn_rest_poller;
pub use signals::{Signal, SignalDetector, SignalType};

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

    // Background tasks
    _rest_poller: Option<tokio::task::JoinHandle<()>>,
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

        Self {
            config,
            state_store,
            predictor,
            data_logger: logger_handle,
            markets: Vec::new(),
            _rest_poller: None,
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

    /// Update price from WebSocket
    pub fn update_ws_price(&self, token_id: &str, bid: Decimal, ask: Decimal) {
        self.state_store.update_ws_price(token_id, bid, ask);

        // Find market for this token and log
        for market in &self.markets {
            if market.up_token_id == token_id || market.down_token_id == token_id {
                let event = LogEvent::price_update(
                    &market.market_id,
                    token_id,
                    "ws",
                    Some(bid),
                    Some(ask),
                );
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
    pub fn scan(&self) -> Vec<ArbPrediction> {
        let mut predictions = Vec::new();

        for state in self.state_store.all_states() {
            if let Some(prediction) = self.predictor.predict(&state) {
                // Log signals
                for signal in &prediction.signals {
                    let event = LogEvent::signal(&state.market_id, signal);
                    self.data_logger.log_nonblocking(event);
                }

                // Log prediction
                let event = LogEvent::prediction(&prediction);
                self.data_logger.log_nonblocking(event);

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
        let max_exposure = self.config.max_exposure_per_market;

        // Calculate current total exposure (max of either side)
        let current_exposure = current_up_shares.max(current_down_shares);

        // Calculate remaining room before hitting max exposure
        let remaining_room = (max_exposure - current_exposure).max(Decimal::ZERO);

        if remaining_room <= Decimal::ZERO {
            debug!(
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

        debug!(
            base_size = %base_size,
            confidence = %confidence.round_dp(4),
            multiplier = %confidence_multiplier.round_dp(2),
            exposure = %current_exposure,
            room = %remaining_room,
            size = %final_size,
            "Safe arb size calculated"
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

