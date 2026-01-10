use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use polymarket_client_sdk::gamma::types::request::{EventByIdRequest, MarketsRequest};
use polymarket_client_sdk::gamma::types::response::{Event as GammaEvent, Market as GammaMarket};
use polymarket_client_sdk::gamma::Client as GammaClient;
use reqwest::Client as HttpClient;
use std::collections::HashSet;
use tracing::info;

use crate::config::assets::ASSETS_BY_PREFIX;
use crate::models::{MarketIds, MarketInfo};

const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

#[derive(Debug, thiserror::Error)]
pub enum MarketCacheError {
    #[error("API request failed: {0}")]
    Api(String),

    #[error("Failed to parse market data: {0}")]
    Parse(String),
}

pub struct MarketCache {
    client: GammaClient,
    http: HttpClient,
    selector: MarketSelector,
}

impl MarketCache {
    pub fn new_crypto(target_assets: HashSet<String>) -> Self {
        Self {
            client: GammaClient::default(),
            http: HttpClient::new(),
            selector: MarketSelector::Crypto { target_assets },
        }
    }

    pub fn new_sports(series_ids: Vec<String>) -> Self {
        Self {
            client: GammaClient::default(),
            http: HttpClient::new(),
            selector: MarketSelector::Sports { series_ids },
        }
    }

    pub async fn get_markets(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<MarketInfo>, MarketCacheError> {
        match &self.selector {
            MarketSelector::Crypto { target_assets } => {
                self.get_crypto_markets(now, target_assets).await
            }
            MarketSelector::Sports { series_ids } => self.get_sports_markets(now, series_ids).await,
        }
    }

    async fn get_crypto_markets(
        &self,
        now: DateTime<Utc>,
        target_assets: &HashSet<String>,
    ) -> Result<Vec<MarketInfo>, MarketCacheError> {
        let raw_markets = self.fetch_raw_markets().await?;
        info!("Fetched {} raw markets from Gamma API", raw_markets.len());

        let mut seen_assets: HashSet<String> = HashSet::new();
        let mut active_markets = Vec::new();

        for market in raw_markets.into_iter() {
            if let Some(info) = self.convert_market(market, target_assets) {
                if info.start_time <= now && info.end_time > now {
                    if seen_assets.insert(info.asset.clone()) {
                        active_markets.push(info);
                    }
                }
            }
        }

        info!("Filtered to {} valid markets", active_markets.len());
        Ok(active_markets)
    }

    async fn get_sports_markets(
        &self,
        now: DateTime<Utc>,
        series_ids: &[String],
    ) -> Result<Vec<MarketInfo>, MarketCacheError> {
        let mut active_markets = Vec::new();
        let mut seen_events: HashSet<String> = HashSet::new();

        for series_id in series_ids {
            let events = self.fetch_events_for_series(series_id).await?;
            info!(
                series_id = %series_id,
                count = events.len(),
                "Fetched sports events"
            );
            for event in events {
                let event = if event.live.is_none() && event.ended.is_none() {
                    self.fetch_event_by_id(&event.id).await?
                } else {
                    event
                };
                if event.live != Some(true) || event.ended == Some(true) {
                    continue;
                }
                if is_nhl_event(&event) && is_overtime_or_shootout(event.period.as_deref()) {
                    continue;
                }
                if let Some(market) = select_who_will_win_market(&event) {
                    if let Some(info) = self.convert_sports_market(&event, &market, now).await? {
                        if seen_events.insert(event.id.clone()) {
                            active_markets.push(info);
                        }
                    }
                }
            }
        }

        info!("Filtered to {} valid sports markets", active_markets.len());
        Ok(active_markets)
    }

    async fn fetch_raw_markets(&self) -> Result<Vec<GammaMarket>, MarketCacheError> {
        let request = MarketsRequest::builder()
            .tag_id("102467")
            .limit(1200)
            .closed(false)
            .ascending(false)
            .build();

        self.client
            .markets(&request)
            .await
            .map_err(|e| MarketCacheError::Api(e.to_string()))
    }

    fn convert_market(
        &self,
        m: GammaMarket,
        target_assets: &HashSet<String>,
    ) -> Option<MarketInfo> {
        let slug = m.slug.as_deref()?;

        if !is_15m_market(slug) {
            return None;
        }

        let prefix = slug.split('-').next()?.to_ascii_lowercase();
        let asset_info = ASSETS_BY_PREFIX.get(&prefix)?;

        if !target_assets.contains(&asset_info.asset) {
            return None;
        }

        let outcomes_str = m.outcomes.as_deref()?;
        let outcomes: Vec<String> = serde_json::from_str(outcomes_str).ok()?;

        let clob_token_ids_str = m.clob_token_ids.as_deref()?;
        let clob_token_ids: Vec<String> = serde_json::from_str(clob_token_ids_str).ok()?;

        if outcomes.len() != 2 || clob_token_ids.len() != 2 {
            return None;
        }

        let up_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("up"))?;
        let down_idx = outcomes
            .iter()
            .position(|o| o.eq_ignore_ascii_case("down"))?;

        let (start_time, end_time) = parse_15m_window(slug)
            .or_else(|| m.start_date.zip(m.end_date))?;
        let condition_id = m.condition_id.clone()?;

        let ids = MarketIds {
            gamma_id: m.id,
            condition_id,
            up_token: clob_token_ids[up_idx].to_string(),
            down_token: clob_token_ids[down_idx].to_string(),
        };

        Some(MarketInfo {
            ids,
            slug: slug.to_string(),
            asset: asset_info.asset.clone(),
            start_time,
            end_time,
        })
    }

    async fn fetch_events_for_series(
        &self,
        series_id: &str,
    ) -> Result<Vec<GammaEvent>, MarketCacheError> {
        let mut events = Vec::new();
        let mut offset = 0;
        let limit = 100;

        loop {
            let limit_str = limit.to_string();
            let offset_str = offset.to_string();
            let response = self
                .http
                .get(format!("{GAMMA_URL}/events"))
                .query(&[
                    ("series_id", series_id),
                    ("active", "true"),
                    ("closed", "false"),
                    ("limit", limit_str.as_str()),
                    ("offset", offset_str.as_str()),
                ])
                .send()
                .await
                .map_err(|e| MarketCacheError::Api(e.to_string()))?
                .error_for_status()
                .map_err(|e| MarketCacheError::Api(e.to_string()))?;

            let batch: Vec<GammaEvent> = response
                .json()
                .await
                .map_err(|e| MarketCacheError::Api(e.to_string()))?;

            let batch_len = batch.len();
            if batch_len == 0 {
                break;
            }

            for event in batch {
                if event_date_is_today_or_earlier(event.event_date.as_deref()) {
                    events.push(event);
                }
            }

            if batch_len < limit {
                break;
            }

            offset += limit;
        }

        Ok(events)
    }

    async fn fetch_event_by_id(&self, event_id: &str) -> Result<GammaEvent, MarketCacheError> {
        let request = EventByIdRequest::builder().id(event_id).build();
        self.client
            .event_by_id(&request)
            .await
            .map_err(|e| MarketCacheError::Api(e.to_string()))
    }

    async fn convert_sports_market(
        &self,
        event: &GammaEvent,
        market: &GammaMarket,
        now: DateTime<Utc>,
    ) -> Result<Option<MarketInfo>, MarketCacheError> {
        if let Some(event_date) = event.event_date.as_deref() {
            if !event_date_is_today_or_earlier(Some(event_date)) {
                return Ok(None);
            }
        }

        let mut market = market.clone();
        let mut outcomes = parse_json_list(market.outcomes.as_deref());
        let mut clob_token_ids = parse_json_list(market.clob_token_ids.as_deref());

        if market.condition_id.is_none()
            || outcomes.as_ref().map_or(true, |list| list.len() != 2)
            || clob_token_ids.as_ref().map_or(true, |list| list.len() != 2)
        {
            if let Some(hydrated) = self.fetch_market_by_id(&market.id).await? {
                market = hydrated;
                outcomes = parse_json_list(market.outcomes.as_deref());
                clob_token_ids = parse_json_list(market.clob_token_ids.as_deref());
            }
        }

        let Some(outcomes) = outcomes else {
            return Ok(None);
        };
        let Some(clob_token_ids) = clob_token_ids else {
            return Ok(None);
        };
        if outcomes.len() != 2 || clob_token_ids.len() != 2 {
            return Ok(None);
        }

        let Some(condition_id) = market.condition_id.clone() else {
            return Ok(None);
        };
        let start_time = event
            .start_time
            .or(market.event_start_time)
            .or(parse_game_start_time(market.game_start_time.as_deref()))
            .or(market.start_date)
            .or_else(|| parse_event_date_start(event.event_date.as_deref()));
        let Some(start_time) = start_time else {
            return Ok(None);
        };
        if start_time > now {
            tracing::debug!(
                event_id = %event.id,
                market_id = %market.id,
                live = ?event.live,
                ended = ?event.ended,
                event_date = ?event.event_date,
                game_start_time = ?market.game_start_time,
                event_start_time = ?event.start_time,
                market_event_start_time = ?market.event_start_time,
                market_start_date = ?market.start_date,
                market_end_date = ?market.end_date,
                event_end_date = ?event.end_date,
                start_time = %start_time,
                now = %now,
                "Upcoming sports market (kept)"
            );
        }
        let end_time = market
            .end_date
            .or(event.end_date)
            .unwrap_or_else(|| start_time + Duration::hours(4));
        let is_live = event.live == Some(true) && event.ended != Some(true);
        if !is_live && end_time <= now {
            return Ok(None);
        }

        let slug = market
            .slug
            .clone()
            .or_else(|| event.slug.clone())
            .unwrap_or_else(|| market.id.clone());
        let ids = MarketIds {
            gamma_id: market.id.clone(),
            condition_id,
            up_token: clob_token_ids[0].clone(),
            down_token: clob_token_ids[1].clone(),
        };

        Ok(Some(MarketInfo {
            ids,
            slug,
            asset: event.id.clone(),
            start_time,
            end_time,
        }))
    }

    pub async fn fetch_market_by_id(
        &self,
        market_id: &str,
    ) -> Result<Option<GammaMarket>, MarketCacheError> {
        use polymarket_client_sdk::gamma::types::request::MarketByIdRequest;

        let request = MarketByIdRequest::builder().id(market_id).build();

        match self.client.market_by_id(&request).await {
            Ok(market) => Ok(Some(market)),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("404") || err_str.contains("not found") {
                    Ok(None)
                } else {
                    Err(MarketCacheError::Api(err_str))
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum MarketSelector {
    Crypto { target_assets: HashSet<String> },
    Sports { series_ids: Vec<String> },
}

fn is_15m_market(slug: &str) -> bool {
    let parts: Vec<&str> = slug.split('-').collect();
    if parts.len() < 4 {
        return false;
    }
    parts.get(1) == Some(&"updown") && parts.get(2) == Some(&"15m")
}

fn parse_15m_window(slug: &str) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    if !is_15m_market(slug) {
        return None;
    }
    let ts_str = slug.rsplit('-').next()?;
    let ts = ts_str.parse::<i64>().ok()?;
    let start = Utc.timestamp_opt(ts, 0).single()?;
    let end = start + Duration::minutes(15);
    Some((start, end))
}

fn event_date_is_today_or_earlier(event_date: Option<&str>) -> bool {
    let Some(event_date) = event_date else {
        return false;
    };

    let Ok(event_day) = NaiveDate::parse_from_str(event_date, "%Y-%m-%d") else {
        return false;
    };
    let today = Utc::now().date_naive();
    event_day <= today
}

fn parse_json_list(value: Option<&str>) -> Option<Vec<String>> {
    value.and_then(|raw| serde_json::from_str(raw).ok())
}

fn parse_event_date_start(event_date: Option<&str>) -> Option<DateTime<Utc>> {
    let event_date = event_date?;
    let day = NaiveDate::parse_from_str(event_date, "%Y-%m-%d").ok()?;
    Some(Utc.from_utc_datetime(&day.and_hms_opt(0, 0, 0)?))
}

fn parse_game_start_time(value: Option<&str>) -> Option<DateTime<Utc>> {
    let raw = value?;
    let normalized = if raw.ends_with("+00") {
        format!("{raw}00")
    } else {
        raw.to_string()
    };
    DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%z")
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn select_who_will_win_market(event: &GammaEvent) -> Option<GammaMarket> {
    let markets = event.markets.as_ref()?;
    let mut candidates: Vec<GammaMarket> = markets
        .iter()
        .filter(|market| {
            market.enable_order_book == Some(true)
                && market.active == Some(true)
                && market.closed == Some(false)
                && market.archived != Some(true)
        })
        .cloned()
        .collect();

    if candidates.is_empty() {
        return None;
    }

    if let Some(moneyline) = candidates
        .iter()
        .find(|market| market.sports_market_type.as_deref() == Some("moneyline"))
    {
        return Some(moneyline.clone());
    }

    candidates.sort_by(|a, b| score_market(b).cmp(&score_market(a)));
    candidates.into_iter().next()
}

fn is_nhl_event(event: &GammaEvent) -> bool {
    event
        .series_slug
        .as_deref()
        .map(|slug| slug.contains("nhl"))
        .unwrap_or(false)
}

fn is_overtime_or_shootout(period: Option<&str>) -> bool {
    matches!(period, Some("OT") | Some("PEN"))
}

fn score_market(market: &GammaMarket) -> (rust_decimal::Decimal, rust_decimal::Decimal) {
    let liquidity = market.liquidity_num.unwrap_or_default();
    let volume = market.volume_num.unwrap_or_default();
    (liquidity, volume)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_15m_market() {
        assert!(is_15m_market("btc-updown-15m-123"));
        assert!(is_15m_market("btc-updown-15m-1767301200"));
        assert!(!is_15m_market("btc-up-or-down-1hr"));
        assert!(!is_15m_market("btc-updown-1hr-123"));
        assert!(!is_15m_market("random-slug"));
    }

    #[test]
    fn test_parse_15m_window() {
        let slug = "btc-updown-15m-1700000000";
        let (start, end) = parse_15m_window(slug).expect("window");
        assert_eq!(start.timestamp(), 1700000000);
        assert_eq!(end.timestamp(), 1700000000 + 15 * 60);
    }
}
