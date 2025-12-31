use futures_util::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::rtds::Client as RtdsClient;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

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
        info!("Binance feed started");
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
            error!("Binance stream error: {}, reconnecting in 1s...", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }
}

async fn run_binance_stream(
    symbols: &[String],
    prices: &Arc<RwLock<HashMap<String, BinancePrice>>>,
) -> anyhow::Result<()> {
    let client = RtdsClient::default();

    let symbols_owned: Vec<String> = symbols.iter().map(|s| s.to_lowercase()).collect();
    info!("Subscribing to Binance prices: {:?}", symbols_owned);

    // rtds::Client.subscribe_crypto_prices takes Option<Vec<String>>
    let stream = client.subscribe_crypto_prices(Some(symbols_owned))?;
    let mut stream = Box::pin(stream);

    while let Some(result) = stream.next().await {
        match result {
            Ok(crypto_price) => {
                // CryptoPrice struct - check actual fields
                let symbol = crypto_price.symbol.to_lowercase();
                let timestamp_ms = crate::models::now_ms();
                
                // Convert price - try different approaches
                let price = Decimal::from_str(&format!("{}", crypto_price.value))
                    .or_else(|_| Decimal::from_str(&format!("{:?}", crypto_price.value)))
                    .unwrap_or(Decimal::ZERO);

                if price > Decimal::ZERO {
                    debug!("Binance {} = {}", symbol, price);

                    prices.write().insert(
                        symbol.clone(),
                        BinancePrice {
                            symbol,
                            price,
                            timestamp_ms,
                        },
                    );
                }
            }
            Err(e) => {
                warn!("Binance price error: {}", e);
            }
        }
    }

    Ok(())
}