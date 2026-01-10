use chrono::{DateTime, Utc};

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
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}
