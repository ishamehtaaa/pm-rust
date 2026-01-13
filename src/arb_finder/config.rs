// arb_finder/config.rs
//
// Self-contained configuration for the arb finder system.
// All arb-related settings live here, not in the main config.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration;

/// Configuration for the arb finder system
#[derive(Debug, Clone)]
pub struct ArbFinderConfig {
    // === Thresholds ===
    /// Combined price below which we consider it an arb opportunity
    pub arb_threshold: Decimal,

    /// Minimum profit per pair to consider worth executing
    pub min_profit_per_pair: Decimal,

    // === Sweep Detection ===
    /// Trade size above which we consider it a "large" trade (potential sweep)
    pub sweep_size_threshold: Decimal,

    /// Multiplier of average trade size to detect sweeps
    pub sweep_size_multiplier: Decimal,

    // === Imbalance Detection ===
    /// Ratio of depth (heavy_side / thin_side) to trigger imbalance signal
    pub imbalance_ratio_threshold: Decimal,

    /// Minimum depth difference to consider significant
    pub min_depth_difference: Decimal,

    // === Velocity Detection ===
    /// Price change per second to consider "fast" movement
    pub velocity_threshold: Decimal,

    /// Window for measuring price velocity
    pub velocity_window: Duration,

    // === REST Polling ===
    /// How often to poll REST API for price snapshots
    pub rest_poll_interval: Duration,

    /// Price difference between REST and WS to consider a discrepancy
    pub rest_ws_discrepancy_threshold: Decimal,

    // === Confidence & Execution ===
    /// Minimum confidence to pre-position orders
    pub pre_position_confidence: Decimal,

    /// Minimum confidence to execute aggressively (IOC/market)
    pub aggressive_confidence: Decimal,

    /// Signal weights for confidence calculation
    pub sweep_weight: Decimal,
    pub imbalance_weight: Decimal,
    pub velocity_weight: Decimal,
    pub discrepancy_weight: Decimal,

    // === Timeouts ===
    /// How long to wait for prediction to materialize before canceling
    pub prediction_timeout: Duration,

    /// Maximum age of market data before considering it stale
    pub max_data_age: Duration,

    // === Position Sizing ===
    /// Default size for arb orders
    pub arb_order_size: Decimal,

    /// Maximum total exposure per market
    pub max_exposure_per_market: Decimal,

    // === Logging ===
    /// Enable detailed data logging for ML training
    pub enable_data_logging: bool,

    /// Path for data log files
    pub data_log_path: String,
}

impl Default for ArbFinderConfig {
    fn default() -> Self {
        Self {
            // Thresholds
            arb_threshold: dec!(0.98),
            min_profit_per_pair: dec!(0.01),

            // Sweep detection
            sweep_size_threshold: dec!(50),
            sweep_size_multiplier: dec!(3.0),

            // Imbalance detection
            imbalance_ratio_threshold: dec!(2.0),
            min_depth_difference: dec!(100),

            // Velocit
            velocity_threshold: dec!(0.02), // 2% per second
            velocity_window: Duration::from_secs(1),

            // REST polling - faster to catch fleeting opportunities
            rest_poll_interval: Duration::from_millis(50),
            rest_ws_discrepancy_threshold: dec!(0.01),

            // Confidence - lower thresholds to act faster on signals
            pre_position_confidence: dec!(0.2),  // Act early
            aggressive_confidence: dec!(0.5),    // Be aggressive
            sweep_weight: dec!(0.4),
            imbalance_weight: dec!(0.3),
            velocity_weight: dec!(0.2),
            discrepancy_weight: dec!(0.1),

            // Timeouts
            prediction_timeout: Duration::from_secs(5),
            max_data_age: Duration::from_secs(1),

            // Position sizing
            arb_order_size: dec!(10),
            max_exposure_per_market: dec!(40),

            // Logging
            enable_data_logging: true,
            data_log_path: "arb_data".to_string(),
        }
    }
}

impl ArbFinderConfig {
    /// Load config from environment variables, falling back to defaults
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Override from env if present
        if let Ok(v) = std::env::var("ARB_THRESHOLD") {
            if let Ok(d) = v.parse() {
                config.arb_threshold = d;
            }
        }

        if let Ok(v) = std::env::var("ARB_ORDER_SIZE") {
            if let Ok(d) = v.parse() {
                config.arb_order_size = d;
            }
        }

        if let Ok(v) = std::env::var("ARB_SWEEP_THRESHOLD") {
            if let Ok(d) = v.parse() {
                config.sweep_size_threshold = d;
            }
        }

        if let Ok(v) = std::env::var("ARB_REST_POLL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                config.rest_poll_interval = Duration::from_millis(ms);
            }
        }

        if let Ok(v) = std::env::var("ARB_DATA_LOGGING") {
            config.enable_data_logging = v == "true" || v == "1";
        }

        if let Ok(v) = std::env::var("ARB_DATA_LOG_PATH") {
            config.data_log_path = v;
        }

        config
    }
}
