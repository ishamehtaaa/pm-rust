// arb_finder/trade_poller.rs
//
// Polls REST API for public trades to detect sweeps by other traders.
// Since there's no public WebSocket for trades, we poll at high frequency.

use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::request::TradesRequest;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{interval, Duration};
use tracing::{debug, info, trace};

use super::config::ArbFinderConfig;
use super::data_logger::{DataLoggerHandle, LogEvent};
use super::market_state::{MarketStateStore, TradeEvent, TradeSide};

type AuthenticatedClient = Client<Authenticated<Normal>>;

/// Token info for trade polling
#[derive(Clone)]
pub struct TradeTokenInfo {
    pub token_id: String,
    pub market_id: String,
}

/// REST trade poller that detects sweeps and large trades by other users
pub struct TradePoller {
    config: ArbFinderConfig,
    client: Arc<AuthenticatedClient>,
    state_store: MarketStateStore,
    data_logger: DataLoggerHandle,
    tokens: Vec<TradeTokenInfo>,
    /// Track last seen trade timestamp per token to only fetch new trades
    last_seen: HashMap<String, i64>,
}

impl TradePoller {
    pub fn new(
        config: ArbFinderConfig,
        client: Arc<AuthenticatedClient>,
        state_store: MarketStateStore,
        data_logger: DataLoggerHandle,
        tokens: Vec<TradeTokenInfo>,
    ) -> Self {
        Self {
            config,
            client,
            state_store,
            data_logger,
            tokens,
            last_seen: HashMap::new(),
        }
    }

    /// Spawn the trade poller as a background task
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(&mut self) {
        // Poll more frequently than REST book poller - trades are time-sensitive
        let poll_interval = Duration::from_millis(50); // 50ms = 20 polls/sec
        let mut ticker = interval(poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!(
            poll_interval_ms = poll_interval.as_millis(),
            token_count = self.tokens.len(),
            "Trade poller started"
        );

        loop {
            ticker.tick().await;
            self.poll_all_tokens().await;
        }
    }

    async fn poll_all_tokens(&mut self) {
        for token in self.tokens.clone() {
            self.poll_token_trades(&token.token_id, &token.market_id).await;
        }
    }

    async fn poll_token_trades(&mut self, token_id: &str, market_id: &str) {
        // Build request - only fetch trades after our last seen timestamp
        let req = if let Some(last_ts) = self.last_seen.get(token_id) {
            TradesRequest::builder()
                .asset_id(token_id)
                .after(*last_ts)
                .build()
        } else {
            TradesRequest::builder()
                .asset_id(token_id)
                .build()
        };

        match self.client.trades(&req, None).await {
            Ok(page) => {
                for trade in page.data {
                    // Parse trade data
                    let price: Decimal = trade.price.to_string().parse().unwrap_or_default();
                    let size: Decimal = trade.size.to_string().parse().unwrap_or_default();
                    let side = match trade.side.to_string().to_lowercase().as_str() {
                        "buy" => TradeSide::Buy,
                        _ => TradeSide::Sell,
                    };

                    // Update last seen timestamp using match_time
                    let ts = trade.match_time.timestamp();
                    if let Some(current) = self.last_seen.get_mut(token_id) {
                        if ts > *current {
                            *current = ts;
                        }
                    } else {
                        self.last_seen.insert(token_id.to_string(), ts);
                    }

                    // Record trade in state store for signal detection
                    let trade_event = TradeEvent {
                        timestamp: Instant::now(),
                        token_id: token_id.to_string(),
                        side,
                        price,
                        size,
                    };
                    self.state_store.record_trade(token_id, trade_event);

                    // Log trade for analysis
                    let side_str = match side {
                        TradeSide::Buy => "buy",
                        TradeSide::Sell => "sell",
                    };
                    let event = LogEvent::trade(market_id, token_id, side_str, price, size);
                    self.data_logger.log_nonblocking(event);

                    // Log large trades (potential sweeps)
                    if size >= self.config.sweep_size_threshold {
                        info!(
                            market_id = %market_id,
                            token_id = %token_id,
                            side = %side_str,
                            price = %price,
                            size = %size,
                            "🚨 LARGE TRADE DETECTED (potential sweep)"
                        );
                    } else {
                        trace!(
                            token_id = %token_id,
                            side = %side_str,
                            price = %price,
                            size = %size,
                            "Trade detected"
                        );
                    }
                }
            }
            Err(e) => {
                debug!(
                    token_id = %token_id,
                    error = %e,
                    "Failed to fetch trades"
                );
            }
        }
    }
}

/// Spawn trade poller for a set of markets
pub fn spawn_trade_poller(
    config: ArbFinderConfig,
    client: Arc<AuthenticatedClient>,
    state_store: MarketStateStore,
    data_logger: DataLoggerHandle,
    markets: Vec<(String, String, String)>, // (market_id, up_token, down_token)
) -> tokio::task::JoinHandle<()> {
    let mut tokens = Vec::new();

    for (market_id, up_token, down_token) in markets {
        tokens.push(TradeTokenInfo {
            token_id: up_token,
            market_id: market_id.clone(),
        });
        tokens.push(TradeTokenInfo {
            token_id: down_token,
            market_id,
        });
    }

    let poller = TradePoller::new(config, client, state_store, data_logger, tokens);
    poller.spawn()
}
