use crate::models::{ClobEvent, ClobPriceChangeEvent, ClobBookEvent, Side, TradingPair, now_ms};
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

pub struct ClobPriceFeed {
    token_map: Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
    shutdown_tx: broadcast::Sender<()>,
    reconnect_tx: tokio::sync::mpsc::Sender<()>,
}

impl ClobPriceFeed {
    pub fn new() -> (Self, tokio::sync::mpsc::Receiver<()>) {
        let (shutdown_tx, _) = broadcast::channel(1);
        let (reconnect_tx, reconnect_rx) = tokio::sync::mpsc::channel(1);
        
        (Self {
            token_map: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx,
            reconnect_tx,
        }, reconnect_rx)
    }
    
    pub fn set_pairs(&self, pairs: Vec<Arc<RwLock<TradingPair>>>) {
        let mut token_map = self.token_map.write();
        token_map.clear();
        
        for pair in pairs {
            let p = pair.read();
            token_map.insert(p.up_token_id.clone(), (pair.clone(), Side::Up));
            token_map.insert(p.down_token_id.clone(), (pair.clone(), Side::Down));
        }
        
        info!("Registered {} token mappings", token_map.len());
        
        // Trigger reconnect to resubscribe
        let _ = self.reconnect_tx.try_send(());
    }
    
    pub fn start(&self, mut reconnect_rx: tokio::sync::mpsc::Receiver<()>) -> tokio::task::JoinHandle<()> {
        let token_map = self.token_map.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            loop {
                let ws_result = connect_async(WS_URL).await;
                let (ws_stream, _) = match ws_result {
                    Ok(conn) => conn,
                    Err(e) => {
                        warn!("CLOB WS connect error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        continue;
                    }
                };
                
                info!("CLOB WS connected");
                let (mut write, mut read) = ws_stream.split();
                
                // Subscribe to current tokens
                {
                    let map = token_map.read();
                    let token_ids: Vec<&String> = map.keys().collect();
                    if !token_ids.is_empty() {
                        let sub_msg = serde_json::json!({
                            "type": "market",
                            "assets_ids": token_ids
                        });
                        if let Err(e) = write.send(Message::Text(sub_msg.to_string())).await {
                            warn!("Failed to subscribe: {}", e);
                        } else {
                            info!("Subscribed to {} tokens", token_ids.len());
                        }
                    }
                }
                
                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("CLOB feed shutting down");
                            return;
                        }
                        _ = reconnect_rx.recv() => {
                            info!("Reconnecting to resubscribe");
                            break;
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    Self::handle_message(&text, &token_map);
                                }
                                Some(Ok(Message::Close(_))) => {
                                    info!("CLOB WS closed");
                                    break;
                                }
                                Some(Err(e)) => {
                                    warn!("CLOB WS error: {}", e);
                                    break;
                                }
                                None => break,
                                _ => {}
                            }
                        }
                    }
                }
                
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        })
    }
    
    fn handle_message(
        text: &str,
        token_map: &Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
    ) {
        if text == "INVALID OPERATION" {
            warn!("Received INVALID OPERATION");
            return;
        }
        
        // Try parsing as array first, then single event
        let events: Vec<ClobEvent> = if let Ok(arr) = serde_json::from_str::<Vec<ClobEvent>>(text) {
            arr
        } else if let Ok(evt) = serde_json::from_str::<ClobEvent>(text) {
            vec![evt]
        } else {
            return;
        };
        
        let ts = now_ms();
        let map = token_map.read();
        
        for event in events {
            match event {
                ClobEvent::PriceChange(e) => {
                    for item in e.price_changes {
                        if let Some((pair, side)) = map.get(&item.asset_id) {
                            let bid: Decimal = item.best_bid.parse().unwrap_or_default();
                            let ask: Decimal = item.best_ask.parse().unwrap_or_default();
                            
                            let mut p = pair.write();
                            match side {
                                Side::Up => {
                                    p.up_bid = Some(bid);
                                    p.up_ask = Some(ask);
                                }
                                Side::Down => {
                                    p.down_bid = Some(bid);
                                    p.down_ask = Some(ask);
                                }
                            }
                            p.last_update_ms = ts;
                        }
                    }
                }
                ClobEvent::Book(e) => {
                    if e.bids.is_empty() || e.asks.is_empty() {
                        continue;
                    }
                    if let Some((pair, side)) = map.get(&e.asset_id) {
                        let bid = e.bids.last().map(|l| l.price);
                        let ask = e.asks.first().map(|l| l.price);
                        
                        let mut p = pair.write();
                        match side {
                            Side::Up => {
                                p.up_bid = bid;
                                p.up_ask = ask;
                            }
                            Side::Down => {
                                p.down_bid = bid;
                                p.down_ask = ask;
                            }
                        }
                        p.last_update_ms = ts;
                    }
                }
                _ => {}
            }
        }
    }
    
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }
}
