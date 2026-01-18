use chrono::{DateTime, TimeDelta, Utc};
use polymarket_client_sdk::types::B256;

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub id: String,
    pub condition_id: Option<B256>,
    pub slug: String,
    pub asset: String,
    pub duration: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub up_token_id: String,
    pub down_token_id: String,
}

#[derive(Debug)]
pub struct MarketState {
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
