/*
 * Training Data Logger + Adaptive Parameter Tuning
 * 
 * Logs structured data about each trading decision for offline analysis
 * and model training. Each record captures the full context of a decision
 * and its outcome.
 * 
 * Also includes AdaptiveParams which automatically adjusts thresholds
 * based on recent trading performance.
 */

use rust_decimal::Decimal;
use rust_decimal::prelude::{ToPrimitive, FromPrimitive};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::Mutex;
use tracing::info;


/* 
 * A single training record capturing decision context and outcome.
 * This is what we'll use to train models.
 */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /* Timestamp */
    pub timestamp_ms: i64,
    pub market_id: String,
    
    /* Market state at decision time */
    pub up_bid: f64,
    pub up_ask: f64,
    pub down_bid: f64,
    pub down_ask: f64,
    
    /* Model outputs */
    pub fair_value_up: f64,      /* RN-JD fair value for Up */
    pub conviction: f64,         /* 0-1 trading confidence */
    pub momentum: f64,           /* -1 to +1 price direction */
    pub calmness: f64,           /* 0-1 volatility inverse */
    pub realized_vol: f64,       /* Raw volatility */
    
    /* Decision made */
    pub action: TradeAction,
    pub order_side: Option<String>,  /* "Up" or "Down" */
    pub order_price: Option<f64>,
    pub order_size: Option<f64>,
    
    /* Outcome (filled in later when we know it) */
    pub fill_price: Option<f64>,
    pub fill_size: Option<f64>,
    pub fill_latency_ms: Option<i64>,
    pub profit_cents: Option<f64>,   /* Combined pair profit */
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeAction {
    PlaceOrder,
    CancelOrder,
    Skip,  /* Decided not to trade */
}

/* A completed trade pair record for the jsonl log */
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedTradeRecord {
    pub timestamp_ms: i64,
    pub market_id: String,
    pub action: String,  /* "CompletedPair" */
    pub conviction_at_trade: f64,
    pub momentum_at_trade: f64,
    pub calmness_at_trade: f64,
    pub combined_cost: f64,
    pub profit_cents: f64,
}

/* Logger that appends to a JSONL file */
pub struct TrainingLogger {
    writer: Mutex<Option<BufWriter<File>>>,
    #[allow(dead_code)]
    path: String,
}

impl TrainingLogger {
    pub fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok();
        
        let writer = file.map(BufWriter::new);
        
        Self {
            writer: Mutex::new(writer),
            path: path.to_string(),
        }
    }
    
    pub fn log(&self, record: &TradeRecord) {
        if let Ok(mut guard) = self.writer.lock() {
            if let Some(ref mut writer) = *guard {
                if let Ok(json) = serde_json::to_string(record) {
                    let _ = writeln!(writer, "{}", json);
                    let _ = writer.flush();
                }
            }
        }
    }
    
    /* Log a completed trade pair with profit info */
    pub fn log_completed(&self, trade: &CompletedTrade, market_id: &str) {
        let record = CompletedTradeRecord {
            timestamp_ms: trade.timestamp_ms,
            market_id: market_id.to_string(),
            action: "CompletedPair".to_string(),
            conviction_at_trade: trade.conviction_at_trade,
            momentum_at_trade: trade.momentum_at_trade,
            calmness_at_trade: trade.calmness_at_trade,
            combined_cost: trade.combined_cost,
            profit_cents: trade.profit_cents,
        };
        
        if let Ok(mut guard) = self.writer.lock() {
            if let Some(ref mut writer) = *guard {
                if let Ok(json) = serde_json::to_string(&record) {
                    let _ = writeln!(writer, "{}", json);
                    let _ = writer.flush();
                }
            }
        }
    }
    
    /* Create a record from current market state */
    pub fn create_record(
        market_id: &str,
        up_bid: Decimal,
        up_ask: Decimal,
        down_bid: Decimal,
        down_ask: Decimal,
        summary: Option<&crate::trend_window::RnJdSummary>,
    ) -> TradeRecord {
        let (fair_value_up, conviction, momentum, calmness, realized_vol) = 
            summary.map(|s| {
                let fair = (s.rn_bid + s.rn_ask) / 2.0;
                (fair, s.vol_metrics.conviction, s.vol_metrics.momentum, 
                 s.vol_metrics.calmness, s.vol_metrics.realized_vol)
            }).unwrap_or((0.5, 0.0, 0.0, 0.0, 0.0));
        
        TradeRecord {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            market_id: market_id.to_string(),
            up_bid: up_bid.to_f64().unwrap_or(0.0),
            up_ask: up_ask.to_f64().unwrap_or(0.0),
            down_bid: down_bid.to_f64().unwrap_or(0.0),
            down_ask: down_ask.to_f64().unwrap_or(0.0),
            fair_value_up,
            conviction,
            momentum,
            calmness,
            realized_vol,
            action: TradeAction::Skip,
            order_side: None,
            order_price: None,
            order_size: None,
            fill_price: None,
            fill_size: None,
            fill_latency_ms: None,
            profit_cents: None,
        }
    }
}

impl Default for TrainingLogger {
    fn default() -> Self {
        Self::new("training_data.jsonl")
    }
}

/*
 * ============================================================================
 * ADAPTIVE PARAMETERS - Online Learning
 * ============================================================================
 * 
 * Automatically adjusts trading parameters based on recent performance.
 * Uses exponential moving averages to track what conditions lead to
 * profitable trades and adjusts thresholds accordingly.
 */

/* A completed trade pair for learning */
#[derive(Debug, Clone)]
pub struct CompletedTrade {
    pub conviction_at_trade: f64,
    pub momentum_at_trade: f64,
    pub calmness_at_trade: f64,
    pub combined_cost: f64,
    pub profit_cents: f64,
    pub timestamp_ms: i64,
}

/* Tracks recent trades and learns optimal parameters */
pub struct AdaptiveParams {
    /* Recent trade history (last N trades) */
    recent_trades: VecDeque<CompletedTrade>,
    max_history: usize,
    
    /* Learned parameters (what we adjust) */
    pub min_conviction: f64,
    pub momentum_threshold: f64,
    pub target_combined_cost: f64,
    
    /* Learning rate for exponential smoothing (reserved for future use) */
    #[allow(dead_code)]
    alpha: f64,
    
    /* Statistics */
    pub total_trades: usize,
    pub profitable_trades: usize,
    pub total_profit_cents: f64,
}

impl AdaptiveParams {
    pub fn new() -> Self {
        Self {
            recent_trades: VecDeque::new(),
            max_history: 100,
            
            /* Start with conservative defaults */
            min_conviction: 0.25,
            momentum_threshold: 0.2,
            target_combined_cost: 0.97,
            
            alpha: 0.1,  /* Learning rate: 10% weight to new observation */
            
            total_trades: 0,
            profitable_trades: 0,
            total_profit_cents: 0.0,
        }
    }
    
    /* Record a completed trade and update parameters */
    pub fn record_trade(&mut self, trade: CompletedTrade) {
        let profitable = trade.profit_cents > 0.0;
        
        self.total_trades += 1;
        if profitable {
            self.profitable_trades += 1;
        }
        self.total_profit_cents += trade.profit_cents;
        
        /* Add to history */
        self.recent_trades.push_back(trade.clone());
        if self.recent_trades.len() > self.max_history {
            self.recent_trades.pop_front();
        }
        
        /* Update parameters based on this trade */
        self.update_params(&trade, profitable);
        
        /* Log performance periodically */
        if self.total_trades % 10 == 0 {
            self.log_performance();
        }
    }
    
    fn update_params(&mut self, trade: &CompletedTrade, profitable: bool) {
        /*
         * CONVICTION THRESHOLD LEARNING
         * If we traded at low conviction and lost → raise threshold
         * If we traded at high conviction and won → lower threshold (be more aggressive)
         */
        if profitable {
            /* Successful trade - conviction was sufficient, maybe we can be more aggressive */
            if trade.conviction_at_trade > self.min_conviction + 0.1 {
                /* We had much more conviction than needed - can lower threshold */
                let adjustment = (trade.conviction_at_trade - self.min_conviction) * 0.05;
                self.min_conviction = (self.min_conviction - adjustment).max(0.15);
            }
        } else {
            /* Failed trade - was conviction too low? */
            if trade.conviction_at_trade < 0.5 {
                /* Low conviction trade failed - raise threshold */
                self.min_conviction = (self.min_conviction + 0.02).min(0.6);
            }
        }
        
        /*
         * MOMENTUM THRESHOLD LEARNING
         * Track which momentum levels led to catching good fills
         */
        if profitable && trade.momentum_at_trade.abs() > 0.3 {
            /* Successfully traded during high momentum - our threshold might be too conservative */
            self.momentum_threshold = self.momentum_threshold * 0.95 + trade.momentum_at_trade.abs() * 0.05;
        }
        
        /*
         * COMBINED COST TARGET LEARNING
         * If we're consistently profitable, we can afford tighter margins
         * If we're losing, widen the target
         */
        if self.recent_trades.len() >= 10 {
            let recent_win_rate = self.recent_win_rate();
            if recent_win_rate > 0.7 {
                /* Winning a lot - can be more aggressive on cost */
                self.target_combined_cost = (self.target_combined_cost + 0.002).min(0.99);
            } else if recent_win_rate < 0.4 {
                /* Losing too much - need better prices */
                self.target_combined_cost = (self.target_combined_cost - 0.005).max(0.94);
            }
        }
    }
    
    fn recent_win_rate(&self) -> f64 {
        if self.recent_trades.is_empty() {
            return 0.5;
        }
        let wins = self.recent_trades.iter().filter(|t| t.profit_cents > 0.0).count();
        wins as f64 / self.recent_trades.len() as f64
    }
    
    fn log_performance(&self) {
        let win_rate = if self.total_trades > 0 {
            self.profitable_trades as f64 / self.total_trades as f64
        } else {
            0.0
        };
        
        info!(
            total_trades = self.total_trades,
            win_rate = %format!("{:.1}%", win_rate * 100.0),
            total_profit = %format!("{:.2}¢", self.total_profit_cents),
            min_conviction = %format!("{:.3}", self.min_conviction),
            momentum_thresh = %format!("{:.3}", self.momentum_threshold),
            target_cost = %format!("{:.3}", self.target_combined_cost),
            "Adaptive params update"
        );
    }
    
    /* Get the current min conviction as a Decimal for use in bot */
    pub fn min_conviction_decimal(&self) -> Decimal {
        Decimal::from_f64(self.min_conviction).unwrap_or(rust_decimal_macros::dec!(0.25))
    }
    
    /* Get current target combined cost as a Decimal */
    pub fn target_combined_cost_decimal(&self) -> Decimal {
        Decimal::from_f64(self.target_combined_cost).unwrap_or(rust_decimal_macros::dec!(0.97))
    }
}

impl Default for AdaptiveParams {
    fn default() -> Self {
        Self::new()
    }
}
