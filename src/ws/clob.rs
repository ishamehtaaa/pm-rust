use crate::models::{Side, TradingPair};
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[derive(Debug, Serialize)]
struct SubscribeMessage {
    #[serde(rename = "type")]
    msg_type: String,
    assets_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct PriceChangeItem {
    asset_id: String,
    best_bid: String,
    best_ask: String,
}

#[derive(Debug, Deserialize)]
struct PriceChangeEvent {
    event_type: String,
    market: String,
    price_changes: Vec<PriceChangeItem>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ClobMessage {
    PriceChange(PriceChangeEvent),
    PriceChangeList(Vec<PriceChangeEvent>),
    Other(serde_json::Value),
}

pub struct ClobFeed {
    // token_id -> (TradingPair, Side)
    token_map: Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
    task: Option<JoinHandle<()>>,
}

impl ClobFeed {
    pub fn new() -> Self {
        Self {
            token_map: Arc::new(RwLock::new(HashMap::new())),
            task: None,
        }
    }

    pub fn set_pairs(&mut self, pairs: Vec<Arc<RwLock<TradingPair>>>) {
        let mut map = HashMap::new();

        for pair in pairs {
            let (up_token, down_token, asset) = {
                let p = pair.read();
                (
                    p.up_token_id.clone(),
                    p.down_token_id.clone(),
                    p.asset.clone(),
                )
            };

            map.insert(up_token.clone(), (pair.clone(), Side::Up));
            map.insert(down_token.clone(), (pair.clone(), Side::Down));

            debug!("Registered tokens for {}: UP={}, DOWN={}", asset, up_token, down_token);
        }

        *self.token_map.write() = map;
        info!("CLOB feed tracking {} tokens", self.token_map.read().len());
    }

    pub fn start(&mut self) {
        if self.task.is_some() {
            return;
        }

        let token_map = self.token_map.clone();

        let task = tokio::spawn(async move {
            run_clob_feed(token_map).await;
        });

        self.task = Some(task);
        info!("CLOB feed started");
    }

    pub fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            info!("CLOB feed stopped");
        }
    }

    pub fn clear(&mut self) {
        self.token_map.write().clear();
    }
}

impl Default for ClobFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ClobFeed {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn run_clob_feed(token_map: Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>) {
    loop {
        if let Err(e) = run_clob_stream(&token_map).await {
            error!("CLOB stream error: {}, reconnecting in 1s...", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }
}

async fn run_clob_stream(
    token_map: &Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
) -> anyhow::Result<()> {
    // Get current token IDs to subscribe
    let token_ids: Vec<String> = token_map.read().keys().cloned().collect();

    if token_ids.is_empty() {
        debug!("No tokens to subscribe, waiting 1s...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        return Ok(());
    }

    info!("Connecting to CLOB WebSocket: {}", CLOB_WS_URL);

    let (ws_stream, _) = connect_async(CLOB_WS_URL).await?;
    let (mut write, mut read) = ws_stream.split();

    // Subscribe to market prices
    let subscribe_msg = SubscribeMessage {
        msg_type: "market".to_string(),
        assets_ids: token_ids.clone(),
    };
    let msg_text = serde_json::to_string(&subscribe_msg)?;
    write.send(Message::Text(msg_text.into())).await?;
    info!("Subscribed to {} CLOB tokens", token_ids.len());

    while let Some(msg) = read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("CLOB WS error: {}", e);
                break;
            }
        };

        if let Message::Text(text) = msg {
            if text == "INVALID OPERATION" {
                warn!("Received INVALID OPERATION from CLOB");
                continue;
            }

            let parsed: Result<ClobMessage, _> = serde_json::from_str(&text);
            match parsed {
                Ok(ClobMessage::PriceChange(event)) => {
                    process_price_changes(&event.price_changes, token_map);
                }
                Ok(ClobMessage::PriceChangeList(events)) => {
                    for event in events {
                        process_price_changes(&event.price_changes, token_map);
                    }
                }
                Ok(ClobMessage::Other(_)) => {
                    // Ignore other message types (book, trade, etc)
                }
                Err(e) => {
                    debug!("Failed to parse CLOB message: {}", e);
                }
            }
        }
    }

    Ok(())
}

fn process_price_changes(
    items: &[PriceChangeItem],
    token_map: &Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
) {
    let ts_ms = crate::models::now_ms();

    for item in items {
        let map = token_map.read();
        if let Some((pair, side)) = map.get(&item.asset_id) {
            let bid = Decimal::from_str(&item.best_bid).ok();
            let ask = Decimal::from_str(&item.best_ask).ok();

            if let (Some(bid_val), Some(ask_val)) = (bid, ask) {
                let mut p = pair.write();

                match side {
                    Side::Up => {
                        p.up_bid = Some(bid_val);
                        p.up_ask = Some(ask_val);
                    }
                    Side::Down => {
                        p.down_bid = Some(bid_val);
                        p.down_ask = Some(ask_val);
                    }
                }
                p.last_update_ms = ts_ms;

                debug!("{} {:?} bid={} ask={}", p.asset, side, bid_val, ask_val);
            }
        }
    }
}