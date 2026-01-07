use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
pub struct MarketIds {
    pub gamma_id: String,
    pub condition_id: String,
    pub up_token: String,
    pub down_token: String,
}

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub ids: MarketIds,
    pub slug: String,
    pub asset: String,
    pub binance_symbol: String,
    pub duration: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

pub fn duration_label(duration: Duration) -> String {
    let minutes = duration.num_minutes().abs();
    if minutes < 60 {
        format!("{}m", minutes)
    } else {
        let hours = minutes / 60;
        format!("{}h", hours)
    }
}
