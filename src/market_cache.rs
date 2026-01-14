use crate::config::{ASSETS_BY_PREFIX, MarketDuration};
use crate::models::{duration_label, MarketInfo};
use chrono::{DateTime, Utc};
use polymarket_client_sdk::gamma::types::request::MarketsRequest;
use polymarket_client_sdk::gamma::types::response::Market as GammaMarket;
use polymarket_client_sdk::gamma::Client as GammaClient;
use std::collections::HashSet;
use tracing::info;

#[derive(Debug, thiserror::Error)]
pub enum MarketCacheError {
    #[error("API request failed: {0}")]
    Api(String),

    #[error("Failed to parse market data: {0}")]
    Parse(String),
}

pub struct MarketCache {
    client: GammaClient,
    target_assets: HashSet<String>,
    market_duration: MarketDuration,
}

impl MarketCache {
    pub fn new(target_assets: HashSet<String>, market_duration: MarketDuration) -> Self {
        info!(
            duration = %market_duration,
            tag_id = market_duration.tag_id(),
            "MarketCache initialized"
        );
        Self {
            client: GammaClient::default(),
            target_assets,
            market_duration,
        }
    }

    pub async fn get_markets(&self, _now: DateTime<Utc>) -> Result<Vec<MarketInfo>, MarketCacheError> {
        let raw_markets = self.fetch_raw_markets().await?;
        info!("Fetched {} raw markets from Gamma API", raw_markets.len());

        let markets: Vec<MarketInfo> = raw_markets
            .into_iter()
            .filter_map(|m| self.convert_market(m))
            .collect();

        info!("Filtered to {} valid markets", markets.len());
        Ok(markets)
    }

    async fn fetch_raw_markets(&self) -> Result<Vec<GammaMarket>, MarketCacheError> {
        let request = MarketsRequest::builder()
            .tag_id(self.market_duration.tag_id())
            .limit(1200)
            .closed(false)
            .ascending(false)
            .build();

        self.client
            .markets(&request)
            .await
            .map_err(|e| MarketCacheError::Api(e.to_string()))
    }

    fn convert_market(&self, m: GammaMarket) -> Option<MarketInfo> {
        let slug = m.slug.as_deref()?;
         
        // Check if slug matches the configured duration pattern
        if !self.market_duration.matches_slug(slug) {
            return None;
        }

        // Extract asset prefix based on duration pattern
        let prefix = self.market_duration.extract_asset_prefix(slug)?;
        let asset_info = ASSETS_BY_PREFIX.get(&prefix)?;

        // Must be a target asset
        if !self.target_assets.contains(&asset_info.asset) {
            return None;
        }

        // Parse outcomes from JSON string
        let outcomes_str = m.outcomes.as_deref()?;
        let outcomes: Vec<String> = serde_json::from_str(outcomes_str).ok()?;

        let clob_token_ids_str = m.clob_token_ids.as_deref()?;
        let clob_token_ids: Vec<String> = serde_json::from_str(clob_token_ids_str).ok()?;

        if outcomes.len() != 2 || clob_token_ids.len() != 2 {
            return None;
        }

        let up_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("up"))?;
        let down_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("down"))?;

        let start_time = m.start_date?;
        let end_time = m.end_date?;

        let duration = duration_label(end_time - start_time);

        Some(MarketInfo {
            id: m.id,
            slug: slug.to_string(),
            asset: asset_info.asset.clone(),
            duration,
            start_time,
            end_time,
            up_token_id: clob_token_ids[up_idx].to_string(),
            down_token_id: clob_token_ids[down_idx].to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_15m_market_matching() {
        let duration = MarketDuration::FifteenMin;
        assert!(duration.matches_slug("btc-updown-15m-123"));
        assert!(duration.matches_slug("btc-updown-15m-1767301200"));
        assert!(!duration.matches_slug("btc-up-or-down-1hr"));
        assert!(!duration.matches_slug("btc-updown-1hr-123"));
        assert!(!duration.matches_slug("random-slug"));
        assert!(!duration.matches_slug("ethereum-up-or-down-january-13-6pm-et"));
    }

    #[test]
    fn test_1hr_market_matching() {
        let duration = MarketDuration::OneHour;
        assert!(duration.matches_slug("ethereum-up-or-down-january-13-6pm-et"));
        assert!(duration.matches_slug("bitcoin-up-or-down-january-14-3am-et"));
        assert!(duration.matches_slug("solana-up-or-down-february-1-12pm-et"));
        assert!(!duration.matches_slug("btc-updown-15m-123"));
        assert!(!duration.matches_slug("random-slug"));
    }

    #[test]
    fn test_asset_extraction_15m() {
        let duration = MarketDuration::FifteenMin;
        assert_eq!(duration.extract_asset_prefix("btc-updown-15m-123"), Some("btc".to_string()));
        assert_eq!(duration.extract_asset_prefix("eth-updown-15m-456"), Some("eth".to_string()));
        assert_eq!(duration.extract_asset_prefix("sol-updown-15m-789"), Some("sol".to_string()));
    }

    #[test]
    fn test_asset_extraction_1hr() {
        let duration = MarketDuration::OneHour;
        assert_eq!(duration.extract_asset_prefix("ethereum-up-or-down-january-13-6pm-et"), Some("ethereum".to_string()));
        assert_eq!(duration.extract_asset_prefix("bitcoin-up-or-down-january-14-3am-et"), Some("bitcoin".to_string()));
        assert_eq!(duration.extract_asset_prefix("solana-up-or-down-february-1-12pm-et"), Some("solana".to_string()));
    }
}
