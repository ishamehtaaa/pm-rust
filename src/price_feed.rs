// price_feed.rs
use futures::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::request::OrderBookSummaryRequest;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{debug, warn};

use crate::models::MarketEvent;
const WINDOW_SIZE: usize = 10;
const TRIM_COUNT: usize = 1;

/// Shared price state updated by WebSocket feed
#[derive(Debug, Default)]
pub struct PriceCache {
    /// token_id -> (bid, ask, timestamp_ms)
    prices: HashMap<String, (Decimal, Decimal, u64)>,
    windows: HashMap<String, PriceWindow>,
}

/// Shared REST price state (no smoothing)
#[derive(Debug, Default)]
pub struct RestPriceCache {
    /// token_id -> (bid, ask, timestamp_ms)
    prices: HashMap<String, (Decimal, Decimal, u64)>,
}

#[derive(Debug, Default)]
struct PriceWindow {
    bids: VecDeque<Decimal>,
    asks: VecDeque<Decimal>,
}

impl PriceCache {
    pub fn get(&self, token_id: &str) -> Option<(Decimal, Decimal)> {
        self.prices.get(token_id).map(|(bid, ask, _)| (*bid, *ask))
    }

    pub fn get_with_age(&self, token_id: &str, now_ms: u64) -> Option<(Decimal, Decimal, u64)> {
        self.prices
            .get(token_id)
            .map(|(bid, ask, ts)| (*bid, *ask, now_ms.saturating_sub(*ts)))
    }

    fn update(&mut self, token_id: String, bid: Decimal, ask: Decimal, timestamp_ms: u64) {
        let window = self.windows.entry(token_id.clone()).or_default();
        push_window(&mut window.bids, bid);
        push_window(&mut window.asks, ask);

        let effective_bid = trimmed_mean(&window.bids).unwrap_or(bid);
        let effective_ask = trimmed_mean(&window.asks).unwrap_or(ask);

        self.prices
            .insert(token_id, (effective_bid, effective_ask, timestamp_ms));
    }
}

impl RestPriceCache {
    pub fn get_with_age(&self, token_id: &str, now_ms: u64) -> Option<(Decimal, Decimal, u64)> {
        self.prices
            .get(token_id)
            .map(|(bid, ask, ts)| (*bid, *ask, now_ms.saturating_sub(*ts)))
    }

    fn update(&mut self, token_id: String, bid: Decimal, ask: Decimal, timestamp_ms: u64) {
        self.prices.insert(token_id, (bid, ask, timestamp_ms));
    }
}

fn push_window(window: &mut VecDeque<Decimal>, value: Decimal) {
    if window.len() == WINDOW_SIZE {
        window.pop_front();
    }
    window.push_back(value);
}

fn trimmed_mean(values: &VecDeque<Decimal>) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }

    let mut sorted: Vec<Decimal> = values.iter().copied().collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

    let len = sorted.len();
    let trim = if len > 2 * TRIM_COUNT { TRIM_COUNT } else { 0 };
    let slice = &sorted[trim..(len - trim)];

    if slice.is_empty() {
        return None;
    }

    let sum: Decimal = slice.iter().copied().sum();
    Some(sum / Decimal::from(slice.len() as u32))
}

pub fn spawn_price_feed(
    ws_endpoint: &str,
    asset_ids: Vec<String>,
    cache: Arc<RwLock<PriceCache>>,
    event_tx: mpsc::Sender<MarketEvent>,
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
                    let best_bid = book.bids.last().map(|l| l.price);

                    // Best ask = lowest ask (cheapest offer to sell)
                    let best_ask = book.asks.last().map(|l| l.price);

                    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                        let bid_dec: Decimal = bid.to_string().parse().unwrap_or_default();
                        let ask_dec: Decimal = ask.to_string().parse().unwrap_or_default();
                        let ts: u64 = book.timestamp.try_into().unwrap_or(0);

                        let asset_id = book.asset_id;
                        cache.write().update(asset_id.clone(), bid_dec, ask_dec, ts);
                        let _ = event_tx.try_send(MarketEvent::Price(asset_id));
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

type AuthenticatedClient = Client<Authenticated<Normal>>;

pub fn spawn_rest_price_poller(
    client: Arc<AuthenticatedClient>,
    token_ids: Vec<String>,
    cache: Arc<RwLock<RestPriceCache>>,
    event_tx: mpsc::Sender<MarketEvent>,
    poll_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if token_ids.is_empty() {
            warn!("No token IDs for REST orderbook polling");
            return;
        }

        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let requests: Vec<OrderBookSummaryRequest> = token_ids
            .iter()
            .cloned()
            .map(|token_id| {
                OrderBookSummaryRequest::builder()
                    .token_id(token_id)
                    .build()
            })
            .collect();

        loop {
            interval.tick().await;

            match client.order_books(&requests).await {
                Ok(books) => {
                    for book in books {
                        let best_bid = book.bids.iter().map(|l| l.price).max();
                        let best_ask = book.asks.iter().map(|l| l.price).min();
                        if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
                            let ts = book.timestamp.timestamp_millis().max(0) as u64;
                            let asset_id = book.asset_id.clone();
                            cache.write().update(asset_id.clone(), bid, ask, ts);
                            let _ = event_tx.try_send(MarketEvent::Price(asset_id));
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "REST orderbook poll failed");
                }
            }
        }
    })
}
