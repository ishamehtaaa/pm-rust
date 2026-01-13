// arb_finder/rest_poller.rs
//
// Polls REST API for price snapshots to capture opportunities that WS might miss.

use rust_decimal::Decimal;
use std::time::Duration;
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::config::ArbFinderConfig;
use super::market_state::MarketStateStore;
use futures_util::stream::{self, StreamExt};

/// Token info for REST polling
#[derive(Clone)]
pub struct TokenInfo {
    pub token_id: String,
    pub market_id: String,
}

/// REST price poller that runs in background
pub struct RestPoller {
    config: ArbFinderConfig,
    state_store: MarketStateStore,
    tokens: Vec<TokenInfo>,
    client: reqwest::Client,
}

const REST_POLL_CONCURRENCY: usize = 8;
const REST_REQUEST_TIMEOUT: Duration = Duration::from_millis(800);

impl RestPoller {
    pub fn new(
        config: ArbFinderConfig,
        state_store: MarketStateStore,
        tokens: Vec<TokenInfo>,
    ) -> Self {
        Self {
            config,
            state_store,
            tokens,
            client: reqwest::Client::new(),
        }
    }

    /// Spawn the REST poller as a background task
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&self) {
        let mut ticker = interval(self.config.rest_poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;
            self.poll_all_tokens().await;
        }
    }

    async fn poll_all_tokens(&self) {
        let client = self.client.clone();
        let state_store = self.state_store.clone();

        stream::iter(self.tokens.clone())
            .map(move |token| {
                let client = client.clone();
                let state_store = state_store.clone();
                async move {
                    if let Some((bid, ask)) = fetch_best_bid_ask(&client, &token.token_id).await {
                        state_store.update_rest_price(&token.token_id, bid, ask);
                        trace!(
                            token_id = %token.token_id,
                            bid = %bid,
                            ask = %ask,
                            "REST price update"
                        );
                    }
                }
            })
            .buffer_unordered(REST_POLL_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
    }
}

#[derive(Debug, serde::Deserialize)]
struct BookResponse {
    #[serde(default)]
    asks: Vec<BookLevel>,
    #[serde(default)]
    bids: Vec<BookLevel>,
}

#[derive(Debug, serde::Deserialize)]
struct BookLevel {
    price: String,
    size: String,
}

/// Fetch best bid + best ask for a token from REST orderbook.
///
/// Important: we intentionally do NOT rely on array ordering.
/// We compute best bid as max(bids.price), best ask as min(asks.price).
async fn fetch_best_bid_ask(client: &reqwest::Client, token_id: &str) -> Option<(Decimal, Decimal)> {
    let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);

    let response = match client.get(&url).timeout(REST_REQUEST_TIMEOUT).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(token_id = %token_id, error = %e, "REST price request failed");
            return None;
        }
    };

    if !response.status().is_success() {
        debug!(
            token_id = %token_id,
            status = %response.status(),
            "REST price fetch failed"
        );
        return None;
    }

    let book = match response.json::<BookResponse>().await {
        Ok(b) => b,
        Err(e) => {
            debug!(token_id = %token_id, error = %e, "Failed to parse book response");
            return None;
        }
    };

    let best_bid = book
        .bids
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .max();

    let best_ask = book
        .asks
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .min();

    match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some((bid, ask)),
        _ => None,
    }
}

/// Spawn REST poller for a set of markets
pub fn spawn_rest_poller(
    config: ArbFinderConfig,
    state_store: MarketStateStore,
    markets: Vec<(String, String, String)>, // (market_id, up_token, down_token)
) -> tokio::task::JoinHandle<()> {
    let mut tokens = Vec::new();

    for (market_id, up_token, down_token) in markets {
        tokens.push(TokenInfo {
            token_id: up_token,
            market_id: market_id.clone(),
        });
        tokens.push(TokenInfo {
            token_id: down_token,
            market_id,
        });
    }

    let poller = RestPoller::new(config, state_store, tokens);
    poller.spawn()
}

