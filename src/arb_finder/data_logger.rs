// arb_finder/data_logger.rs
//
// Logs market events and predictions for future ML training.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::config::ArbFinderConfig;
use super::predictor::ArbPrediction;
use super::signals::{Signal, SignalDetails};

/// Event types for logging
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum LogEvent {
    /// Price update from any source
    PriceUpdate {
        timestamp_ms: u64,
        market_id: String,
        token_id: String,
        source: String, // "ws" or "rest"
        bid: Option<String>,
        ask: Option<String>,
    },

    /// Trade observed
    Trade {
        timestamp_ms: u64,
        market_id: String,
        token_id: String,
        side: String, // "buy" or "sell"
        price: String,
        size: String,
    },

    /// Signal detected
    SignalDetected {
        timestamp_ms: u64,
        market_id: String,
        signal_type: String,
        strength: String,
        details: String, // JSON-encoded details
    },

    /// Prediction made
    Prediction {
        timestamp_ms: u64,
        market_id: String,
        confidence: String,
        action: String,
        up_target: Option<String>,
        down_target: Option<String>,
        signal_count: usize,
    },

    /// Outcome of a prediction (for training feedback)
    PredictionOutcome {
        timestamp_ms: u64,
        market_id: String,
        prediction_timestamp_ms: u64,
        arb_occurred: bool,
        actual_combined_price: Option<String>,
        profit_captured: Option<String>,
    },

    /// Combined spread snapshot (periodic)
    SpreadSnapshot {
        timestamp_ms: u64,
        market_id: String,
        up_ws_ask: Option<String>,
        down_ws_ask: Option<String>,
        up_rest_ask: Option<String>,
        down_rest_ask: Option<String>,
        combined_ws: Option<String>,
        combined_rest: Option<String>,
    },
}

impl LogEvent {
    fn timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn price_update(
        market_id: &str,
        token_id: &str,
        source: &str,
        bid: Option<Decimal>,
        ask: Option<Decimal>,
    ) -> Self {
        LogEvent::PriceUpdate {
            timestamp_ms: Self::timestamp_ms(),
            market_id: market_id.to_string(),
            token_id: token_id.to_string(),
            source: source.to_string(),
            bid: bid.map(|d| d.to_string()),
            ask: ask.map(|d| d.to_string()),
        }
    }

    pub fn trade(
        market_id: &str,
        token_id: &str,
        side: &str,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        LogEvent::Trade {
            timestamp_ms: Self::timestamp_ms(),
            market_id: market_id.to_string(),
            token_id: token_id.to_string(),
            side: side.to_string(),
            price: price.to_string(),
            size: size.to_string(),
        }
    }

    pub fn signal(market_id: &str, signal: &Signal) -> Self {
        let details = match &signal.details {
            SignalDetails::Sweep {
                token_id,
                trade_side,
                trade_size,
                average_size,
            } => format!(
                "{{\"token_id\":\"{}\",\"trade_side\":\"{:?}\",\"trade_size\":\"{}\",\"average_size\":\"{}\"}}",
                token_id, trade_side, trade_size, average_size
            ),
            SignalDetails::Imbalance { thin_side, ratio } => {
                format!(
                    "{{\"thin_side\":\"{:?}\",\"ratio\":\"{}\"}}",
                    thin_side, ratio
                )
            }
            SignalDetails::Velocity {
                lagging_side,
                fast_velocity,
                slow_velocity,
            } => format!(
                "{{\"lagging_side\":\"{:?}\",\"fast_velocity\":\"{}\",\"slow_velocity\":\"{}\"}}",
                lagging_side, fast_velocity, slow_velocity
            ),
            SignalDetails::Discrepancy {
                token_id,
                rest_price,
                ws_price,
                diff,
            } => format!(
                "{{\"token_id\":\"{}\",\"rest_price\":\"{}\",\"ws_price\":\"{}\",\"diff\":\"{}\"}}",
                token_id, rest_price, ws_price, diff
            ),
            SignalDetails::Composite {
                score,
                momentum,
                volatility,
                imbalance,
                time_to_end_secs,
            } => format!(
                "{{\"score\":\"{}\",\"momentum\":\"{}\",\"volatility\":\"{}\",\"imbalance\":\"{}\",\"time_to_end_secs\":{}}}",
                score, momentum, volatility, imbalance, time_to_end_secs
            ),
        };

        LogEvent::SignalDetected {
            timestamp_ms: Self::timestamp_ms(),
            market_id: market_id.to_string(),
            signal_type: format!("{:?}", signal.signal_type),
            strength: signal.strength.to_string(),
            details,
        }
    }

    pub fn prediction(prediction: &ArbPrediction) -> Self {
        LogEvent::Prediction {
            timestamp_ms: Self::timestamp_ms(),
            market_id: prediction.market_id.clone(),
            confidence: prediction.confidence.to_string(),
            action: format!("{:?}", prediction.recommended_action),
            up_target: prediction.up_target_price.map(|d| d.to_string()),
            down_target: prediction.down_target_price.map(|d| d.to_string()),
            signal_count: prediction.signals.len(),
        }
    }

    pub fn spread_snapshot(
        market_id: &str,
        up_ws_ask: Option<Decimal>,
        down_ws_ask: Option<Decimal>,
        up_rest_ask: Option<Decimal>,
        down_rest_ask: Option<Decimal>,
    ) -> Self {
        let combined_ws = match (up_ws_ask, down_ws_ask) {
            (Some(u), Some(d)) => Some((u + d).to_string()),
            _ => None,
        };
        let combined_rest = match (up_rest_ask, down_rest_ask) {
            (Some(u), Some(d)) => Some((u + d).to_string()),
            _ => None,
        };

        LogEvent::SpreadSnapshot {
            timestamp_ms: Self::timestamp_ms(),
            market_id: market_id.to_string(),
            up_ws_ask: up_ws_ask.map(|d| d.to_string()),
            down_ws_ask: down_ws_ask.map(|d| d.to_string()),
            up_rest_ask: up_rest_ask.map(|d| d.to_string()),
            down_rest_ask: down_rest_ask.map(|d| d.to_string()),
            combined_ws,
            combined_rest,
        }
    }
}

/// Handle for logging events
#[derive(Clone)]
pub struct DataLoggerHandle {
    tx: mpsc::Sender<LogEvent>,
}

impl DataLoggerHandle {
    pub async fn log(&self, event: LogEvent) {
        if let Err(e) = self.tx.send(event).await {
            debug!(error = %e, "Failed to send log event");
        }
    }

    /// Non-blocking log - may drop events if channel is full
    pub fn log_nonblocking(&self, event: LogEvent) {
        if let Err(e) = self.tx.try_send(event) {
            debug!(error = %e, "Failed to send log event (non-blocking)");
        }
    }
}

/// Background data logger
pub struct DataLogger {
    config: ArbFinderConfig,
    rx: mpsc::Receiver<LogEvent>,
    writer: Option<BufWriter<File>>,
    events_written: u64,
}

impl DataLogger {
    pub fn new(config: ArbFinderConfig) -> (Self, DataLoggerHandle) {
        let (tx, rx) = mpsc::channel(1000);

        let logger = Self {
            config,
            rx,
            writer: None,
            events_written: 0,
        };

        let handle = DataLoggerHandle { tx };

        (logger, handle)
    }

    /// Spawn the logger as a background task
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&mut self) {
        if !self.config.enable_data_logging {
            info!("Data logging disabled");
            // Still consume events to prevent channel backup
            while self.rx.recv().await.is_some() {}
            return;
        }

        // Open log file
        if let Err(e) = self.open_log_file() {
            error!(error = %e, "Failed to open log file, logging disabled");
            while self.rx.recv().await.is_some() {}
            return;
        }

        info!(path = %self.config.data_log_path, "Data logging started");

        while let Some(event) = self.rx.recv().await {
            self.write_event(&event);
        }

        // Flush on shutdown
        if let Some(ref mut writer) = self.writer {
            let _ = writer.flush();
        }
    }

    fn open_log_file(&mut self) -> std::io::Result<()> {
        // Create directory if needed
        let path = PathBuf::from(&self.config.data_log_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Generate filename with date
        let now = chrono::Utc::now();
        let filename = format!(
            "{}/arb_data_{}.jsonl",
            self.config.data_log_path,
            now.format("%Y%m%d_%H%M%S")
        );

        // Create directory for the log path itself
        std::fs::create_dir_all(&self.config.data_log_path)?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filename)?;

        self.writer = Some(BufWriter::new(file));
        info!(filename = %filename, "Log file opened");

        Ok(())
    }

    fn write_event(&mut self, event: &LogEvent) {
        if let Some(ref mut writer) = self.writer {
            match serde_json::to_string(event) {
                Ok(json) => {
                    if let Err(e) = writeln!(writer, "{}", json) {
                        warn!(error = %e, "Failed to write log event");
                    } else {
                        self.events_written += 1;

                        // Flush periodically
                        if self.events_written % 100 == 0 {
                            let _ = writer.flush();
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to serialize log event");
                }
            }
        }
    }
}
