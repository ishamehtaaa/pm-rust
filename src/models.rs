use chrono::{DateTime, TimeDelta, Utc};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub id: String,
    pub slug: String,
    pub asset: String,
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
            rest_up_ask: None,
            rest_down_ask: None,
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

    pub rest_up_ask: Option<Decimal>,
    pub rest_down_ask: Option<Decimal>,
    pub last_rest_update_ms: i64,
}

impl TradingPair {
    pub fn latest_up_ask(&self) -> Option<Decimal> {
        self.rest_up_ask
    }

    pub fn latest_down_ask(&self) -> Option<Decimal> {
        self.rest_down_ask
    }

    pub fn combined_ask(&self) -> Option<Decimal> {
        Some(self.latest_up_ask()? + self.latest_down_ask()?)
    }

    pub fn clear_prices(&mut self) {
        self.rest_up_ask = None;
        self.rest_down_ask = None;
        self.last_rest_update_ms = 0;
    }
}

#[derive(Debug)]
pub struct MarketState {
    pub pair: Arc<RwLock<TradingPair>>,
    pub info: MarketInfo,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
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
