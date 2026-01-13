// price_feed.rs
use futures::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, trace, warn};

/// A single price level in the order book
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Full order book depth for a single token
#[derive(Debug, Clone, Default)]
pub struct BookDepth {
    /// Ask levels sorted by price ascending (best ask first)
    pub asks: Vec<PriceLevel>,
    /// Bid levels sorted by price descending (best bid first)
    pub bids: Vec<PriceLevel>,
    /// Timestamp of last update
    pub timestamp: Option<Instant>,
}

impl BookDepth {
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.price)
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|l| l.price)
    }

    /// Total ask liquidity
    pub fn total_ask_size(&self) -> Decimal {
        self.asks.iter().map(|l| l.size).sum()
    }

    /// Total bid liquidity
    pub fn total_bid_size(&self) -> Decimal {
        self.bids.iter().map(|l| l.size).sum()
    }
}

/// Shared price state updated by WebSocket feed
/// Stores RAW prices and full depth - no smoothing, to capture momentary mispricings
#[derive(Debug, Default)]
pub struct PriceCache {
    /// token_id -> (bid, ask, timestamp_ms) - backward compatible best prices
    prices: HashMap<String, (Decimal, Decimal, u64)>,
    /// token_id -> full order book depth
    depths: HashMap<String, BookDepth>,
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

    /// Get full order book depth for a token
    pub fn get_depth(&self, token_id: &str) -> Option<&BookDepth> {
        self.depths.get(token_id)
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

    /// Store full order book depth
    fn update_depth(&mut self, token_id: String, depth: BookDepth) {
        trace!(
            token_id = %token_id,
            ask_levels = depth.asks.len(),
            bid_levels = depth.bids.len(),
            "Depth update"
        );
        self.depths.insert(token_id, depth);
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
                    // Parse ALL ask levels (sorted by price ascending - best ask first)
                    let mut asks: Vec<PriceLevel> = book.asks.iter()
                        .filter_map(|l| {
                            let price: Decimal = l.price.to_string().parse().ok()?;
                            let size: Decimal = l.size.to_string().parse().ok()?;
                            Some(PriceLevel { price, size })
                        })
                        .collect();
                    asks.sort_by(|a, b| a.price.cmp(&b.price));

                    // Parse ALL bid levels (sorted by price descending - best bid first)
                    let mut bids: Vec<PriceLevel> = book.bids.iter()
                        .filter_map(|l| {
                            let price: Decimal = l.price.to_string().parse().ok()?;
                            let size: Decimal = l.size.to_string().parse().ok()?;
                            Some(PriceLevel { price, size })
                        })
                        .collect();
                    bids.sort_by(|a, b| b.price.cmp(&a.price)); // Descending for bids

                    let best_bid = bids.first().map(|l| l.price);
                    let best_ask = asks.first().map(|l| l.price);

                    let ts: u64 = book.timestamp.try_into().unwrap_or(0);

                    // Create depth snapshot
                    let depth = BookDepth {
                        asks,
                        bids,
                        timestamp: Some(Instant::now()),
                    };

                    // Update cache with both best prices and full depth
                    let mut cache_guard = cache.write();
                    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                        cache_guard.update(book.asset_id.clone(), bid, ask, ts);
                    }
                    cache_guard.update_depth(book.asset_id, depth);
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
