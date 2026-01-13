// price_feed.rs
use crate::arb_finder::{MarketStateStore, PriceLevel};
use futures::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// Shared price state updated by WebSocket feed
/// Stores RAW prices - no smoothing, to capture momentary mispricings
#[derive(Debug, Default)]
pub struct PriceCache {
    /// token_id -> (bid, ask, timestamp_ms)
    prices: HashMap<String, (Decimal, Decimal, u64)>,
}

impl PriceCache {
    pub fn get(&self, token_id: &str) -> Option<(Decimal, Decimal)> {
        self.prices.get(token_id).map(|(bid, ask, _)| (*bid, *ask))
    }

    pub fn get_with_age(&self, token_id: &str, now_ms: u64) -> Option<(Decimal, Decimal, u64)> {
        self.prices.get(token_id).map(|(bid, ask, ts)| {
            (*bid, *ask, now_ms.saturating_sub(*ts))
        })
    }

    /// Store raw prices directly - no smoothing to preserve arb opportunities
    fn update(&mut self, token_id: String, bid: Decimal, ask: Decimal, timestamp_ms: u64) {
        trace!(
            token_id = %token_id,
            bid = %bid,
            ask = %ask,
            "Price update (raw)"
        );
        self.prices.insert(token_id, (bid, ask, timestamp_ms));
    }
}

/// Spawn price feed that captures FULL order book depth from WebSocket
/// This is critical for finding arb opportunities at any price level, not just top-of-book
pub fn spawn_price_feed(
    ws_endpoint: &str,
    asset_ids: Vec<String>,
    cache: Arc<RwLock<PriceCache>>,
    state_store: Option<MarketStateStore>,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let client = WsClient::new(ws_endpoint, Default::default())?;

    let handle = tokio::spawn(async move {
        // Subscribe inside the async block so client lives as long as stream
        let stream = match client.subscribe_orderbook(asset_ids) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to subscribe to orderbook :(");
                return;
            }
        };

        let mut stream = Box::pin(stream);

        while let Some(result) = stream.next().await {
            match result {
                Ok(book) => {
                    // Convert FULL depth to our PriceLevel format
                    let bids: Vec<PriceLevel> = book.bids.iter()
                        .filter_map(|l| {
                            let price: Decimal = l.price.to_string().parse().ok()?;
                            let size: Decimal = l.size.to_string().parse().ok()?;
                            Some(PriceLevel { price, size })
                        })
                        .collect();
                    
                    let asks: Vec<PriceLevel> = book.asks.iter()
                        .filter_map(|l| {
                            let price: Decimal = l.price.to_string().parse().ok()?;
                            let size: Decimal = l.size.to_string().parse().ok()?;
                            Some(PriceLevel { price, size })
                        })
                        .collect();

                    let best_bid = bids.iter().map(|l| l.price).max();
                    let best_ask = asks.iter().map(|l| l.price).min();

                    // Update basic price cache (best bid/ask only)
                    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                        let ts: u64 = book.timestamp.try_into().unwrap_or(0);
                        cache.write().update(book.asset_id.clone(), bid, ask, ts);
                    }

                    // Update FULL depth in state store for cross-product arb scanning
                    if let Some(ref store) = state_store {
                        store.update_ws_depth(&book.asset_id, bids, asks);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Price feed error");
                }
            }
        }

        debug!("Price feed stream ended");
    });

    Ok(handle)
}
