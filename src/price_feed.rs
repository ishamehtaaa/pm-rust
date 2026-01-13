// price_feed.rs
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

pub fn spawn_price_feed(
    ws_endpoint: &str,
    asset_ids: Vec<String>,
    cache: Arc<RwLock<PriceCache>>,
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
                    let best_bid = book.bids.iter()
                        .map(|l| l.price)
                        .max();

                    // Best ask = lowest ask (cheapest offer to sell)
                    let best_ask = book.asks.iter()
                        .map(|l| l.price)
                        .min();

                    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                        let bid_dec: Decimal = match bid.to_string().parse() {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(error = %e, asset_id = %book.asset_id, "Failed to parse WS bid");
                                continue;
                            }
                        };
                        let ask_dec: Decimal = match ask.to_string().parse() {
                            Ok(v) => v,
                            Err(e) => {
                                warn!(error = %e, asset_id = %book.asset_id, "Failed to parse WS ask");
                                continue;
                            }
                        };
                        let ts: u64 = book.timestamp.try_into().unwrap_or(0);

                        cache.write().update(book.asset_id, bid_dec, ask_dec, ts);
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
