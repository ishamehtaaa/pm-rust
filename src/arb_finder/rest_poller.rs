// arb_finder/rest_poller.rs
//
// Polls REST API for price snapshots to capture opportunities that WS might miss.

use rust_decimal::Decimal;
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::config::ArbFinderConfig;
use super::market_state::MarketStateStore;

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
            if let Some(price) = self.fetch_price(&token.token_id).await {
                self.state_store.update_rest_price(&token.token_id, price);
                trace!(
                    token_id = %token.token_id,
                    price = %price,
                    "REST price update"
                );
            }
        }
    }

    async fn fetch_price(&self, token_id: &str) -> Option<Decimal> {
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
                        "REST price fetch failed"
                    );
                    return None;
                }

                match response.json::<BookResponse>().await {
                    Ok(book) => {
                        // Get best ask from response
                        book.asks
                            .first()
                            .and_then(|level| level.price.parse::<Decimal>().ok())
                    }
                    Err(e) => {
                        debug!(error = %e, "Failed to parse book response");
                        None
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "REST price request failed");
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

