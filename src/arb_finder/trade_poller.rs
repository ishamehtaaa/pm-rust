// arb_finder/trade_poller.rs
//
// Polls REST API for recent trades to feed sweep/velocity signals.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use chrono::Utc;
use futures_util::stream::{self, StreamExt};
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::clob::types::request::TradesRequest;
use tokio::sync::Mutex;
use tracing::{debug, trace};

use super::config::ArbFinderConfig;
use super::data_logger::{DataLoggerHandle, LogEvent};
use super::market_state::{MarketStateStore, TradeEvent, TradeSide};
use super::MarketInfo;

type AuthenticatedClient = Client<Authenticated<Normal>>;

const TRADE_POLL_CONCURRENCY: usize = 6;
const INITIAL_LOOKBACK_SECS: i64 = 5;

pub fn spawn_trade_poller(
    config: ArbFinderConfig,
    client: Arc<AuthenticatedClient>,
    state_store: MarketStateStore,
    data_logger: DataLoggerHandle,
    markets: Vec<MarketInfo>,
) -> tokio::task::JoinHandle<()> {
    let token_ids: HashSet<String> = markets
        .iter()
        .flat_map(|m| vec![m.up_token_id.clone(), m.down_token_id.clone()])
        .collect();

    let market_by_token: HashMap<String, String> = markets
        .into_iter()
        .flat_map(|m| {
            vec![
                (m.up_token_id.clone(), m.market_id.clone()),
                (m.down_token_id.clone(), m.market_id.clone()),
            ]
        })
        .collect();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.rest_poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let now = Utc::now().timestamp();
        let last_seen: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(
            token_ids
                .iter()
                .map(|t| (t.clone(), now - INITIAL_LOOKBACK_SECS))
                .collect(),
        ));

        loop {
            interval.tick().await;

            stream::iter(token_ids.iter().cloned())
                .map(|token_id| {
                    let client = client.clone();
                    let state_store = state_store.clone();
                    let data_logger = data_logger.clone();
                    let market_by_token = market_by_token.clone();
                    let last_seen = last_seen.clone();
                    async move {
                        let after = {
                            let guard = last_seen.lock().await;
                            *guard.get(&token_id).unwrap_or(&0)
                        };

                        let request = TradesRequest::builder()
                            .asset_id(token_id.clone())
                            .after(after)
                            .build();

                        let page = match client.trades(&request, None).await {
                            Ok(p) => p,
                            Err(e) => {
                                debug!(token_id = %token_id, error = %e, "Trade poll failed");
                                return;
                            }
                        };

                        let mut max_ts = after;
                        for trade in page.data {
                            let trade_ts = trade.match_time.timestamp();
                            if trade_ts <= after {
                                continue;
                            }

                            let side = match trade.side { 
                                Side::Buy => TradeSide::Buy,
                                Side::Sell => TradeSide::Sell,
                                Side::Unknown => continue,
                                _ => continue,
                            };

                            state_store.record_trade(
                                &trade.asset_id,
                                TradeEvent {
                                    timestamp: std::time::Instant::now(),
                                    token_id: trade.asset_id.clone(),
                                    side,
                                    price: trade.price,
                                    size: trade.size,
                                },
                            );

                            if let Some(market_id) = market_by_token.get(&trade.asset_id) {
                                let side_str = match side {
                                    TradeSide::Buy => "buy",
                                    TradeSide::Sell => "sell",
                                };
                                data_logger.log_nonblocking(LogEvent::trade(
                                    market_id,
                                    &trade.asset_id,
                                    side_str,
                                    trade.price,
                                    trade.size,
                                ));
                            }

                            trace!(
                                token_id = %trade.asset_id,
                                side = ?trade.side,
                                price = %trade.price,
                                size = %trade.size,
                                "Trade observed"
                            );

                            if trade_ts > max_ts {
                                max_ts = trade_ts;
                            }
                        }

                        if max_ts > after {
                            let mut guard = last_seen.lock().await;
                            guard.insert(token_id, max_ts);
                        }
                    }
                })
                .buffer_unordered(TRADE_POLL_CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
        }
    })
}
