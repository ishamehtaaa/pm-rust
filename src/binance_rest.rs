use chrono::{DateTime, Utc};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tracing::{debug, warn};

const BINANCE_API_BASE: &str = "https://api.binance.com";

#[derive(Debug, Clone)]
pub struct Kline {
    pub open_time: i64,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
    pub close_time: i64,
}

pub struct BinanceRest {
    client: Client,
}

impl BinanceRest {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    /// Fetch the kline that contains the given timestamp.
    /// For a 15m market starting at T, we want the 15m candle that opens at T.
    pub async fn get_kline_at(
        &self,
        symbol: &str,
        interval: &str,
        start_time: DateTime<Utc>,
    ) -> anyhow::Result<Option<Kline>> {
        let start_ms = start_time.timestamp_millis();
        
        // Fetch klines starting from this time, limit 1
        let url = format!(
            "{}/api/v3/klines?symbol={}&interval={}&startTime={}&limit=1",
            BINANCE_API_BASE,
            symbol.to_uppercase(),
            interval,
            start_ms
        );

        let response = self.client.get(&url).send().await?;
        
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            warn!("Binance API error: {} - {}", status, body);
            return Ok(None);
        }

        let klines: Vec<Vec<serde_json::Value>> = response.json().await?;
        
        if klines.is_empty() {
            return Ok(None);
        }

        let k = &klines[0];
        // Binance kline format: [open_time, open, high, low, close, volume, close_time, ...]
        if k.len() < 7 {
            return Ok(None);
        }

        let open_time = k[0].as_i64().unwrap_or(0);
        let open = parse_decimal(&k[1])?;
        let high = parse_decimal(&k[2])?;
        let low = parse_decimal(&k[3])?;
        let close = parse_decimal(&k[4])?;
        let volume = parse_decimal(&k[5])?;
        let close_time = k[6].as_i64().unwrap_or(0);

        Ok(Some(Kline {
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
        }))
    }

    /// Get current price from Binance ticker
    pub async fn get_price(&self, symbol: &str) -> anyhow::Result<Decimal> {
        let url = format!(
            "{}/api/v3/ticker/price?symbol={}",
            BINANCE_API_BASE,
            symbol.to_uppercase()
        );

        let response = self.client.get(&url).send().await?;
        
        if !response.status().is_success() {
            anyhow::bail!("Binance price API error: {}", response.status());
        }

        #[derive(Deserialize)]
        struct TickerPrice {
            price: String,
        }

        let ticker: TickerPrice = response.json().await?;
        let price = Decimal::from_str(&ticker.price)?;
        
        Ok(price)
    }
}

impl Default for BinanceRest {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_decimal(v: &serde_json::Value) -> anyhow::Result<Decimal> {
    let s = v.as_str().unwrap_or("0");
    Decimal::from_str(s).map_err(|e| anyhow::anyhow!("decimal parse: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_price() {
        let client = BinanceRest::new();
        let price = client.get_price("BTCUSDT").await;
        assert!(price.is_ok());
        println!("BTC price: {:?}", price.unwrap());
    }
}
