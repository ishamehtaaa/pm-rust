use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::debug;

const CHAINLINK_API: &str = "https://data.chain.link/api/query-timescale";

// Chainlink Data Streams feed IDs
// Find these at: https://data.chain.link/streams
pub fn get_feed_id(asset: &str) -> Option<&'static str> {
    match asset.to_lowercase().as_str() {
        "bitcoin" | "btc" => Some("0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"),
        "ethereum" | "eth" => Some("0x000362205e10b3a147d02792eccee483dca6c7b44ecce7012cb8c6e0b68b3ae9"),
        "solana" | "sol" => Some("0x0003b778d3f6b2ac4991302b89cb313f99a42467d6c9c5f96f57c29c0d2bc24f"),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct ChainlinkPrice {
    pub price: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: ApiData,
}

#[derive(Debug, Deserialize)]
struct ApiData {
    #[serde(rename = "liveStreamReports")]
    live_stream_reports: LiveStreamReports,
}

#[derive(Debug, Deserialize)]
struct LiveStreamReports {
    nodes: Vec<ReportNode>,
}

#[derive(Debug, Deserialize)]
struct ReportNode {
    #[serde(rename = "validFromTimestamp")]
    valid_from_timestamp: String,
    price: String,
    bid: String,
    ask: String,
}

pub struct ChainlinkClient {
    client: Client,
}

impl ChainlinkClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    pub async fn get_latest_price(&self, asset: &str) -> anyhow::Result<ChainlinkPrice> {
        let feed_id = get_feed_id(asset)
            .ok_or_else(|| anyhow::anyhow!("Unknown asset: {}", asset))?;

        let reports = self.fetch_reports(feed_id).await?;
        
        if reports.is_empty() {
            anyhow::bail!("No price data for {}", asset);
        }

        Ok(reports.into_iter().next().unwrap())
    }

    pub async fn get_price_at(
        &self,
        asset: &str,
        target_time: DateTime<Utc>,
    ) -> anyhow::Result<ChainlinkPrice> {
        let feed_id = get_feed_id(asset)
            .ok_or_else(|| anyhow::anyhow!("Unknown asset: {}", asset))?;

        let reports = self.fetch_reports(feed_id).await?;
        
        // Find the report closest to but not after target_time
        let mut best: Option<ChainlinkPrice> = None;
        
        for report in reports {
            if report.timestamp <= target_time {
                match &best {
                    None => best = Some(report),
                    Some(b) if report.timestamp > b.timestamp => best = Some(report),
                    _ => {}
                }
            }
        }

        best.ok_or_else(|| anyhow::anyhow!("No price data at or before {}", target_time))
    }

    async fn fetch_reports(&self, feed_id: &str) -> anyhow::Result<Vec<ChainlinkPrice>> {
        // URL-encode the variables JSON manually (just need to encode braces and quotes)
        let variables = format!(r#"{{"feedId":"{}"}}"#, feed_id);
        let encoded = variables
            .replace('{', "%7B")
            .replace('}', "%7D")
            .replace('"', "%22")
            .replace(':', "%3A");

        let url = format!(
            "{}?query=LIVE_STREAM_REPORTS_QUERY&variables={}",
            CHAINLINK_API,
            encoded
        );

        debug!("Chainlink URL: {}", url);

        let response = self.client
            .get(&url)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Chainlink API error: {} - {}", status, body);
        }

        let api_response: ApiResponse = response.json().await?;
        
        let mut prices = Vec::new();
        for node in api_response.data.live_stream_reports.nodes {
            if let Ok(price) = self.parse_report(&node) {
                prices.push(price);
            }
        }

        Ok(prices)
    }

    fn parse_report(&self, node: &ReportNode) -> anyhow::Result<ChainlinkPrice> {
        // Prices have 18 decimal places
        let divisor = Decimal::from_str("1000000000000000000")?;
        
        let price_raw = Decimal::from_str(&node.price)?;
        let bid_raw = Decimal::from_str(&node.bid)?;
        let ask_raw = Decimal::from_str(&node.ask)?;

        let price = price_raw / divisor;
        let bid = bid_raw / divisor;
        let ask = ask_raw / divisor;

        let timestamp = DateTime::parse_from_rfc3339(&node.valid_from_timestamp)?
            .with_timezone(&Utc);

        Ok(ChainlinkPrice {
            price,
            bid,
            ask,
            timestamp,
        })
    }
}

impl Default for ChainlinkClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_latest_price() {
        let client = ChainlinkClient::new();
        
        let btc = client.get_latest_price("bitcoin").await;
        println!("BTC: {:?}", btc);
        assert!(btc.is_ok());
        
        let eth = client.get_latest_price("ethereum").await;
        println!("ETH: {:?}", eth);
        assert!(eth.is_ok());
    }
}