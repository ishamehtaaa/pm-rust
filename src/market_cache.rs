use crate::config::ASSETS_BY_PREFIX;
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
}

impl MarketCache {
    pub fn new(target_assets: HashSet<String>) -> Self {
        Self {
            client: GammaClient::default(),
            target_assets,
        }
    }

    pub async fn get_markets(&self, now: DateTime<Utc>) -> Result<Vec<MarketInfo>, MarketCacheError> {
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

    fn convert_market(&self, m: GammaMarket) -> Option<MarketInfo> {
        let slug = m.slug.as_deref()?;
         
        if !is_15m_market(slug) {
            return None;
        }

        let prefix = slug.split('-').next()?.to_ascii_lowercase();
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
            binance_symbol: asset_info.binance.clone(),
            duration,
            start_time,
            end_time,
            up_token_id: clob_token_ids[up_idx].to_string(),
            down_token_id: clob_token_ids[down_idx].to_string(),
        })
    }



    pub async fn fetch_market_by_id(&self, market_id: &str) -> Result<Option<GammaMarket>, MarketCacheError> {
        use polymarket_client_sdk::gamma::types::request::MarketByIdRequest;
        
        let request = MarketByIdRequest::builder()
            .id(market_id)
            .build();

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


fn is_15m_market(slug: &str) -> bool {
    // Match pattern: {asset}-updown-15m-{number}
    let parts: Vec<&str> = slug.split('-').collect();
    if parts.len() < 4 {
        return false;
    }
    parts.get(1) == Some(&"updown") && parts.get(2) == Some(&"15m")
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
}