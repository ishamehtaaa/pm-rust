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
            up_bid: None,
            up_ask: None,
            down_bid: None,
            down_ask: None,
            last_update_ms: 0,
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

    pub up_bid: Option<Decimal>,
    pub up_ask: Option<Decimal>,
    pub down_bid: Option<Decimal>,
    pub down_ask: Option<Decimal>,
    pub last_update_ms: i64,
}

impl TradingPair {
    pub fn combined_ask(&self) -> Option<Decimal> {
        Some(self.up_ask? + self.down_ask?)
    }

    pub fn has_both_asks(&self) -> bool {
        self.up_ask.is_some() && self.down_ask.is_some()
    }

    pub fn clear_prices(&mut self) {
        self.up_bid = None;
        self.up_ask = None;
        self.down_bid = None;
        self.down_ask = None;
        self.last_update_ms = 0;
    }
}

impl std::fmt::Display for TradingPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]: {} UP: {:?} DOWN: {:?}",
            self.asset.to_uppercase(),
            self.duration,
            self.up_ask,
            self.down_ask
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