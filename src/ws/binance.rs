use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/ws";

#[derive(Debug, Clone)]
pub struct BinancePrice {
    pub symbol: String,
    pub price: Decimal,
    pub timestamp_ms: i64,
}

pub struct BinanceFeed {
    symbols: Vec<String>,
    prices: Arc<RwLock<HashMap<String, BinancePrice>>>,
    task: Option<JoinHandle<()>>,
}

impl BinanceFeed {
    pub fn new(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            prices: Arc::new(RwLock::new(HashMap::new())),
            task: None,
        }
    }

    pub fn start(&mut self) {
        if self.task.is_some() {
            return;
        }

        let symbols = self.symbols.clone();
        let prices = self.prices.clone();

        let task = tokio::spawn(async move {
            run_binance_feed(symbols, prices).await;
        });

        self.task = Some(task);
        info!("Binance WebSocket feed started");
    }

    pub fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            info!("Binance feed stopped");
        }
    }

    pub fn get_price(&self, symbol: &str) -> Option<BinancePrice> {
        let symbol_lower = symbol.to_lowercase();
        self.prices.read().get(&symbol_lower).cloned()
    }
}

impl Drop for BinanceFeed {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn run_binance_feed(
    symbols: Vec<String>,
    prices: Arc<RwLock<HashMap<String, BinancePrice>>>,
) {
    loop {
        if let Err(e) = run_binance_stream(&symbols, &prices).await {
            error!("Binance WebSocket error: {}, reconnecting in 2s...", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    }
}

#[derive(Debug, Deserialize)]
struct MiniTickerEvent {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    close_price: String,
    #[serde(rename = "E")]
    event_time: i64,
}

async fn run_binance_stream(
    symbols: &[String],
    prices: &Arc<RwLock<HashMap<String, BinancePrice>>>,
) -> anyhow::Result<()> {
    if symbols.is_empty() {
        warn!("No symbols to subscribe");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        return Ok(());
    }

    // Build combined stream URL for mini tickers
    // Format: wss://stream.binance.com:9443/stream?streams=btcusdt@miniTicker/ethusdt@miniTicker
    let streams: Vec<String> = symbols
        .iter()
        .map(|s| format!("{}@miniTicker", s.to_lowercase()))
        .collect();
    
    let url = format!("wss://stream.binance.com:9443/stream?streams={}", streams.join("/"));
    
    info!("Connecting to Binance: {}", url);

    let (ws_stream, _) = connect_async(&url).await?;
    let (mut _write, mut read) = ws_stream.split();

    info!("Binance WebSocket connected, subscribed to: {:?}", symbols);

    while let Some(msg) = read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("Binance WS error: {}", e);
                break;
            }
        };

        if let Message::Text(text) = msg {
            // Combined stream format: {"stream":"btcusdt@miniTicker","data":{...}}
            if let Ok(wrapper) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(data) = wrapper.get("data") {
                    if let Ok(ticker) = serde_json::from_value::<MiniTickerEvent>(data.clone()) {
                        if let Ok(price) = Decimal::from_str(&ticker.close_price) {
                            let symbol_lower = ticker.symbol.to_lowercase();
                            
                            debug!("Binance {} = {}", symbol_lower, price);
                            
                            prices.write().insert(
                                symbol_lower.clone(),
                                BinancePrice {
                                    symbol: symbol_lower,
                                    price,
                                    timestamp_ms: ticker.event_time,
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}