// arb_finder/rest_poller.rs
//
// Polls REST API for price snapshots to capture opportunities that WS might miss.
// Now captures FULL DEPTH for cross-product arb detection.

use rust_decimal::Decimal;
use std::time::Instant;
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::config::ArbFinderConfig;
use super::market_state::{DepthSnapshot, MarketStateStore, PriceLevel};

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
        for token in &self.tokens {
            if let Some((best_ask, depth)) = self.fetch_book(&token.token_id).await {
                // Update best ask price (backward compatible)
                self.state_store.update_rest_price(&token.token_id, best_ask);
                
                // Update full depth snapshot
                self.state_store.update_depth(&token.token_id, depth);
                
                trace!(
                    token_id = %token.token_id,
                    best_ask = %best_ask,
                    "REST depth update"
                );
            }
        }
    }

    async fn fetch_book(&self, token_id: &str) -> Option<(Decimal, DepthSnapshot)> {
        let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);

        match self.client.get(&url).send().await {
            Ok(response) => {
                if !response.status().is_success() {
                    debug!(
                        token_id = %token_id,
                        status = %response.status(),
                        "REST book fetch failed"
                    );
                    return None;
                }

                match response.json::<BookResponse>().await {
                    Ok(book) => {
                        // Parse ALL ask levels (sorted by price ascending - best ask first)
                        let mut asks: Vec<PriceLevel> = book.asks.iter()
                            .filter_map(|l| {
                                let price = l.price.parse::<Decimal>().ok()?;
                                let size = l.size.parse::<Decimal>().ok()?;
                                Some(PriceLevel { price, size })
                            })
                            .collect();
                        asks.sort_by(|a, b| a.price.cmp(&b.price));

                        // Parse ALL bid levels (sorted by price descending - best bid first)
                        let mut bids: Vec<PriceLevel> = book.bids.iter()
                            .filter_map(|l| {
                                let price = l.price.parse::<Decimal>().ok()?;
                                let size = l.size.parse::<Decimal>().ok()?;
                                Some(PriceLevel { price, size })
                            })
                            .collect();
                        bids.sort_by(|a, b| b.price.cmp(&a.price)); // Descending for bids

                        let best_ask = asks.first().map(|l| l.price)?;
                        
                        let depth = DepthSnapshot {
                            asks,
                            bids,
                            timestamp: Some(Instant::now()),
                        };

                        Some((best_ask, depth))
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to parse book response");
                        None
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "REST book request failed");
                None
            }
        }
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
