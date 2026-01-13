// arb_finder/rest_poller.rs
//
// Polls REST API for FULL order book depth to capture arb opportunities.
// This is critical because arbs exist at various price levels, not just top-of-book.

use rust_decimal::Decimal;
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::config::ArbFinderConfig;
use super::market_state::{MarketStateStore, PriceLevel};

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
        // Poll all tokens in parallel for speed
        let futures: Vec<_> = self.tokens.iter().map(|token| {
            self.fetch_and_update_depth(token)
        }).collect();
        
        futures::future::join_all(futures).await;
    }

    async fn fetch_and_update_depth(&self, token: &TokenInfo) {
        match self.fetch_full_book(&token.token_id).await {
            Some((bids, asks)) => {
                let best_ask = asks.iter().map(|l| l.price).min();
                let ask_levels = asks.len();
                let bid_levels = bids.len();
                
                // Update state store with full depth
                self.state_store.update_rest_depth(&token.token_id, bids, asks);
                
                trace!(
                    token_id = %token.token_id,
                    best_ask = ?best_ask,
                    ask_levels = ask_levels,
                    bid_levels = bid_levels,
                    "REST depth update"
                );
            }
            None => {
                debug!(token_id = %token.token_id, "Failed to fetch book depth");
            }
        }
    }

    /// Fetch full order book depth (all levels, not just best)
    async fn fetch_full_book(&self, token_id: &str) -> Option<(Vec<PriceLevel>, Vec<PriceLevel>)> {
        let url = format!(
            "https://clob.polymarket.com/book?token_id={}",
            token_id
        );

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
                        // Convert ALL levels to PriceLevel structs
                        let bids: Vec<PriceLevel> = book.bids
                            .iter()
                            .filter_map(|level| {
                                let price = level.price.parse::<Decimal>().ok()?;
                                let size = level.size.parse::<Decimal>().ok()?;
                                Some(PriceLevel { price, size })
                            })
                            .collect();
                        
                        let asks: Vec<PriceLevel> = book.asks
                            .iter()
                            .filter_map(|level| {
                                let price = level.price.parse::<Decimal>().ok()?;
                                let size = level.size.parse::<Decimal>().ok()?;
                                Some(PriceLevel { price, size })
                            })
                            .collect();
                        
                        Some((bids, asks))
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

