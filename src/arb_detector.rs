// arb_detector.rs
//
// Monitors spreads and logs arbitrage opportunities.
// Tracks edge (1.00 - up_ask - down_ask) and alerts when profitable.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::info;

/// Represents a detected arbitrage opportunity
#[derive(Debug, Clone, Serialize)]
pub struct ArbOpportunity {
    pub market_id: String,
    pub asset: String,
    pub up_ask: Decimal,
    pub down_ask: Decimal,
    pub edge: Decimal,
    pub edge_bps: i32,
    pub up_liquidity: Decimal,
    pub down_liquidity: Decimal,
    pub available_pairs: Decimal,
    pub potential_profit: Decimal,
    pub timestamp: DateTime<Utc>,
}

impl ArbOpportunity {
    pub fn new(
        market_id: String,
        asset: String,
        up_ask: Decimal,
        down_ask: Decimal,
        up_liquidity: Decimal,
        down_liquidity: Decimal,
    ) -> Self {
        let edge = dec!(1.00) - up_ask - down_ask;
        let edge_bps = (edge * dec!(10000))
            .to_string()
            .parse::<f64>()
            .unwrap_or(0.0) as i32;
        let available_pairs = up_liquidity.min(down_liquidity);
        let potential_profit = edge * available_pairs;

        Self {
            market_id,
            asset,
            up_ask,
            down_ask,
            edge,
            edge_bps,
            up_liquidity,
            down_liquidity,
            available_pairs,
            potential_profit,
            timestamp: Utc::now(),
        }
    }

    /// Returns true if there's a positive edge
    pub fn is_profitable(&self) -> bool {
        self.edge > Decimal::ZERO
    }
}

/// Configuration for the ArbDetector
#[derive(Debug, Clone)]
pub struct ArbDetectorConfig {
    /// Minimum edge in basis points to trigger an alert
    pub min_edge_bps: i32,
    /// Cooldown between alerts for the same market
    pub alert_cooldown: Duration,
    /// Directory to write JSONL logs
    pub log_dir: String,
    /// Whether to log all opportunities (not just alerts)
    pub log_all: bool,
}

impl Default for ArbDetectorConfig {
    fn default() -> Self {
        Self {
            min_edge_bps: 50, // 0.5% minimum edge to alert
            alert_cooldown: Duration::from_secs(30),
            log_dir: "arb_data".to_string(),
            log_all: true,
        }
    }
}

/// Detects and logs arbitrage opportunities
pub struct ArbDetector {
    config: ArbDetectorConfig,
    log_writer: Option<BufWriter<File>>,
    last_alert: HashMap<String, Instant>,
    /// Rolling stats per market
    edge_history: HashMap<String, EdgeStats>,
}

/// Rolling statistics for edge tracking
#[derive(Debug, Default)]
struct EdgeStats {
    samples: Vec<(Instant, Decimal)>,
    max_window: Duration,
}

impl EdgeStats {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
            max_window: Duration::from_secs(60),
        }
    }

    fn record(&mut self, edge: Decimal) {
        let now = Instant::now();
        self.samples.push((now, edge));
        // Prune old samples
        self.samples
            .retain(|(t, _)| now.duration_since(*t) < self.max_window);
    }

    fn average(&self) -> Option<Decimal> {
        if self.samples.is_empty() {
            return None;
        }
        let sum: Decimal = self.samples.iter().map(|(_, e)| *e).sum();
        Some(sum / Decimal::from(self.samples.len() as u32))
    }

    fn max(&self) -> Option<Decimal> {
        self.samples.iter().map(|(_, e)| *e).max()
    }
}

impl ArbDetector {
    pub fn new(config: ArbDetectorConfig) -> anyhow::Result<Self> {
        // Create log directory if it doesn't exist
        std::fs::create_dir_all(&config.log_dir)?;

        // Create log file with timestamp
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let log_path = Path::new(&config.log_dir).join(format!("arb_opportunities_{}.jsonl", timestamp));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        info!(path = %log_path.display(), "ArbDetector log file created");

        Ok(Self {
            config,
            log_writer: Some(BufWriter::new(file)),
            last_alert: HashMap::new(),
            edge_history: HashMap::new(),
        })
    }

    /// Check for arbitrage opportunity and log/alert if found
    pub fn check_opportunity(
        &mut self,
        market_id: &str,
        asset: &str,
        up_ask: Decimal,
        down_ask: Decimal,
        up_liquidity: Decimal,
        down_liquidity: Decimal,
    ) -> Option<ArbOpportunity> {
        let opp = ArbOpportunity::new(
            market_id.to_string(),
            asset.to_string(),
            up_ask,
            down_ask,
            up_liquidity,
            down_liquidity,
        );

        // Record in edge history
        {
            let stats = self
                .edge_history
                .entry(market_id.to_string())
                .or_insert_with(EdgeStats::new);
            stats.record(opp.edge);
        }

        // Log if configured to log all
        if self.config.log_all {
            self.log_opportunity(&opp);
        }

        // Check if we should alert
        if opp.edge_bps >= self.config.min_edge_bps {
            let should_alert = self
                .last_alert
                .get(market_id)
                .map(|last| last.elapsed() >= self.config.alert_cooldown)
                .unwrap_or(true);

            if should_alert {
                // Get stats snapshot for alert
                let avg_edge = self.average_edge(market_id);
                let max_edge = self.edge_history.get(market_id).and_then(|s| s.max());
                self.emit_alert_with_stats(&opp, avg_edge, max_edge);
                self.last_alert.insert(market_id.to_string(), Instant::now());
            }

            return Some(opp);
        }

        if opp.is_profitable() {
            Some(opp)
        } else {
            None
        }
    }

    /// Log opportunity to JSONL file
    fn log_opportunity(&mut self, opp: &ArbOpportunity) {
        if let Some(ref mut writer) = self.log_writer {
            if let Ok(json) = serde_json::to_string(opp) {
                let _ = writeln!(writer, "{}", json);
                let _ = writer.flush();
            }
        }
    }

    /// Emit an alert for a significant opportunity
    fn emit_alert_with_stats(
        &self,
        opp: &ArbOpportunity,
        avg_edge: Option<Decimal>,
        max_edge: Option<Decimal>,
    ) {
        let avg = avg_edge.unwrap_or(Decimal::ZERO);
        let max = max_edge.unwrap_or(Decimal::ZERO);

        info!(
            asset = %opp.asset,
            market_id = %opp.market_id,
            up_ask = %opp.up_ask,
            down_ask = %opp.down_ask,
            edge_bps = opp.edge_bps,
            potential_profit = %format!("${:.4}", opp.potential_profit),
            available_pairs = %opp.available_pairs,
            avg_edge_60s = %format!("{:.4}", avg),
            max_edge_60s = %format!("{:.4}", max),
            "🎯 ARB OPPORTUNITY DETECTED"
        );
    }

    /// Get current edge for a market (for use in sizing decisions)
    pub fn current_edge(&self, market_id: &str) -> Option<Decimal> {
        self.edge_history
            .get(market_id)
            .and_then(|s| s.samples.last().map(|(_, e)| *e))
    }

    /// Get average edge over last 60 seconds
    pub fn average_edge(&self, market_id: &str) -> Option<Decimal> {
        self.edge_history.get(market_id).and_then(|s| s.average())
    }

    /// Get edge in basis points for a market
    pub fn edge_bps(&self, market_id: &str) -> i32 {
        self.current_edge(market_id)
            .map(|e| (e * dec!(10000)).to_string().parse::<f64>().unwrap_or(0.0) as i32)
            .unwrap_or(0)
    }

    /// Summary stats for all tracked markets
    pub fn summary(&self) -> ArbSummary {
        let mut positive_edge_markets = 0;
        let mut best_edge = Decimal::ZERO;
        let mut best_market = String::new();

        for (market_id, stats) in &self.edge_history {
            if let Some(edge) = stats.samples.last().map(|(_, e)| *e) {
                if edge > Decimal::ZERO {
                    positive_edge_markets += 1;
                }
                if edge > best_edge {
                    best_edge = edge;
                    best_market = market_id.clone();
                }
            }
        }

        ArbSummary {
            total_markets: self.edge_history.len(),
            positive_edge_markets,
            best_edge,
            best_market,
        }
    }
}

#[derive(Debug)]
pub struct ArbSummary {
    pub total_markets: usize,
    pub positive_edge_markets: usize,
    pub best_edge: Decimal,
    pub best_market: String,
}
