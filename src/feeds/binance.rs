use crate::models::{BinancePrice, BinanceTrade, FlowSignal, TradeAccumulator, now_ms};
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn, debug};

pub struct BinanceFeed {
    symbols: Vec<String>,
    prices: Arc<RwLock<HashMap<String, BinancePrice>>>,
    accumulators: Arc<RwLock<HashMap<String, TradeAccumulator>>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl BinanceFeed {
    pub fn new(symbols: Vec<String>) -> Self {
        let symbols: Vec<String> = symbols.iter().map(|s| s.to_lowercase()).collect();
        let mut accumulators = HashMap::new();
        for s in &symbols {
            accumulators.insert(s.clone(), TradeAccumulator::new(5000));
        }
        
        let (shutdown_tx, _) = broadcast::channel(1);
        
        Self {
            symbols,
            prices: Arc::new(RwLock::new(HashMap::new())),
            accumulators: Arc::new(RwLock::new(accumulators)),
            shutdown_tx,
        }
    }
    
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let symbols = self.symbols.clone();
        let prices = self.prices.clone();
        let accumulators = self.accumulators.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        
        tokio::spawn(async move {
            loop {
                let url = if symbols.len() == 1 {
                    format!("wss://stream.binance.com:9443/ws/{}@trade", symbols[0])
                } else {
                    let streams = symbols.iter()
                        .map(|s| format!("{}@trade", s))
                        .collect::<Vec<_>>()
                        .join("/");
                    format!("wss://stream.binance.com:9443/stream?streams={}", streams)
                };
                
                let ws_result = connect_async(&url).await;
                let (ws_stream, _) = match ws_result {
                    Ok(conn) => conn,
                    Err(e) => {
                        warn!("Binance WS connect error: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };
                
                info!("Binance WS connected | symbols: {:?}", symbols);
                let (_, mut read) = ws_stream.split();
                let is_combined = symbols.len() > 1;
                
                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => {
                            info!("Binance feed shutting down");
                            return;
                        }
                        msg = read.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    Self::handle_message(
                                        &text,
                                        is_combined,
                                        &prices,
                                        &accumulators,
                                    );
                                }
                                Some(Ok(Message::Close(_))) => {
                                    info!("Binance WS closed");
                                    break;
                                }
                                Some(Err(e)) => {
                                    warn!("Binance WS error: {}", e);
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
        is_combined: bool,
        prices: &Arc<RwLock<HashMap<String, BinancePrice>>>,
        accumulators: &Arc<RwLock<HashMap<String, TradeAccumulator>>>,
    ) {
        let trade: BinanceTrade = if is_combined {
            #[derive(serde::Deserialize)]
            struct Combined {
                data: BinanceTrade,
            }
            match serde_json::from_str::<Combined>(text) {
                Ok(c) => c.data,
                Err(_) => return,
            }
        } else {
            match serde_json::from_str(text) {
                Ok(t) => t,
                Err(_) => return,
            }
        };
        
        let symbol = trade.symbol_lower();
        let price = trade.price_decimal();
        let event_time = trade.event_time_ms;
        
        {
            let mut prices = prices.write();
            prices.insert(symbol.clone(), BinancePrice {
                symbol: symbol.clone(),
                price,
                timestamp_ms: event_time,
            });
        }
        
        {
            let mut accs = accumulators.write();
            if let Some(acc) = accs.get_mut(&symbol) {
                acc.add_trade(&trade);
            }
        }
    }
    
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(());
    }
    
    pub fn get_price(&self, symbol: &str) -> Option<BinancePrice> {
        self.prices.read().get(&symbol.to_lowercase()).cloned()
    }
    
    pub fn get_flow_signal(&self, symbol: &str) -> Option<FlowSignal> {
        let accs = self.accumulators.read();
        accs.get(&symbol.to_lowercase())?.get_flow_signal()
    }
}
