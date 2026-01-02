use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "Up")]
    Up,
    #[serde(rename = "Down")]
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Opening,
    Midgame,
    Endgame,
    Closed,
}

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub id: String,
    pub slug: String,
    pub asset: String,
    pub binance_symbol: String,
    pub duration: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub up_token_id: String,
    pub down_token_id: String,
}

impl MarketInfo {
    pub fn to_trading_pair(&self) -> TradingPair {
        TradingPair {
            asset: self.asset.clone(),
            duration: self.duration.clone(),
            market_id: self.id.clone(),
            up_token_id: self.up_token_id.clone(),
            down_token_id: self.down_token_id.clone(),
            end_time: self.end_time,
            ws_up_bid: None,
            ws_up_ask: None,
            ws_down_bid: None,
            ws_down_ask: None,
            rest_up_bid: None,
            rest_up_ask: None,
            rest_down_bid: None,
            rest_down_ask: None,
            last_ws_update_ms: 0,
            last_rest_update_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradingPair {
    pub asset: String,
    pub duration: String,
    pub market_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub end_time: DateTime<Utc>,

    // WS-provided prices
    pub ws_up_bid: Option<Decimal>,
    pub ws_up_ask: Option<Decimal>,
    pub ws_down_bid: Option<Decimal>,
    pub ws_down_ask: Option<Decimal>,

    // REST-provided prices
    pub rest_up_bid: Option<Decimal>,
    pub rest_up_ask: Option<Decimal>,
    pub rest_down_bid: Option<Decimal>,
    pub rest_down_ask: Option<Decimal>,

    // Timestamps for last updates from each source
    pub last_ws_update_ms: i64,
    pub last_rest_update_ms: i64,
}

impl TradingPair {
    /// Prefer WS prices if present, otherwise fall back to REST.
    pub fn latest_up_ask(&self) -> Option<Decimal> {
        self.ws_up_ask.or(self.rest_up_ask)
    }

    pub fn latest_down_ask(&self) -> Option<Decimal> {
        self.ws_down_ask.or(self.rest_down_ask)
    }

    pub fn combined_ask(&self) -> Option<Decimal> {
        Some(self.latest_up_ask()? + self.latest_down_ask()?)
    }

    pub fn has_both_asks(&self) -> bool {
        self.latest_up_ask().is_some() && self.latest_down_ask().is_some()
    }

    pub fn clear_prices(&mut self) {
        self.ws_up_bid = None;
        self.ws_up_ask = None;
        self.ws_down_bid = None;
        self.ws_down_ask = None;
        self.rest_up_bid = None;
        self.rest_up_ask = None;
        self.rest_down_bid = None;
        self.rest_down_ask = None;
        self.last_ws_update_ms = 0;
        self.last_rest_update_ms = 0;
    }
}

impl std::fmt::Display for TradingPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]: {} WS_UP: {:?} REST_UP: {:?}",
            self.asset.to_uppercase(),
            self.duration,
            self.ws_up_ask,
            self.rest_up_ask
        )
    }
}

#[derive(Debug)]
pub struct MarketState {
    pub pair: Arc<RwLock<TradingPair>>,
    pub info: MarketInfo,
    pub binance_symbol: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub binance_open_price: Option<Decimal>,
}

impl MarketState {
    pub fn elapsed_seconds(&self, now: DateTime<Utc>) -> f64 {
        (now - self.start_time).num_milliseconds() as f64 / 1000.0
    }

    pub fn remaining_seconds(&self, now: DateTime<Utc>) -> f64 {
        (self.end_time - now).num_milliseconds() as f64 / 1000.0
    }

    pub fn elapsed_pct(&self, now: DateTime<Utc>) -> f64 {
        let total = (self.end_time - self.start_time).num_milliseconds() as f64;
        if total <= 0.0 {
            return 1.0;
        }
        let elapsed = (now - self.start_time).num_milliseconds() as f64;
        (elapsed / total).min(1.0).max(0.0)
    }

    pub fn phase(&self, now: DateTime<Utc>) -> Phase {
        let remaining = self.remaining_seconds(now);
        let elapsed = self.elapsed_seconds(now);

        if remaining <= 0.0 {
            Phase::Closed
        } else if elapsed < 60.0 {
            Phase::Opening
        } else if remaining <= 120.0 {
            Phase::Endgame
        } else {
            Phase::Midgame
        }
    }
}

pub fn duration_label(delta: TimeDelta) -> String {
    let minutes = delta.num_minutes();
    match minutes {
        15 => "15m".into(),
        60 => "1hr".into(),
        240 => "4hr".into(),
        _ => format!("{}m", minutes),
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}