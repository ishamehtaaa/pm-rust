use crate::models::{Side, TradingPair};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::connect_async;

pub struct ClobFeed {
    // token_id -> (TradingPair, Side)
    token_map: Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
    ws_task: Option<JoinHandle<()>>,
    rest_task: Option<JoinHandle<()>>,
}

impl ClobFeed {
    pub fn new() -> Self {
        Self {
            token_map: Arc::new(RwLock::new(HashMap::new())),
            ws_task: None,
            rest_task: None,
        }
    }

    /// Replace tracked pairs. This will be used by the bot to register current markets.
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

    /// Start both SDK WebSocket subscription and a parallel REST poller for comparison.
    pub fn start(&mut self) {
        if self.ws_task.is_some() || self.rest_task.is_some() {
            return;
        }

        let token_map_ws = self.token_map.clone();
        let ws_handle = tokio::spawn(async move {
            if let Err(e) = run_clob_ws(token_map_ws).await {
                error!("CLOB WS task terminated: {}", e);
            }
        });

        let token_map_rest = self.token_map.clone();
        let rest_handle = tokio::spawn(async move {
            run_clob_rest_loop(token_map_rest).await;
        });

        self.ws_task = Some(ws_handle);
        self.rest_task = Some(rest_handle);
        info!("CLOB feed started (WS + REST poller)");
    }

    pub fn stop(&mut self) {
        if let Some(t) = self.ws_task.take() {
            t.abort();
            info!("CLOB WS task stopped");
        }
        if let Some(t) = self.rest_task.take() {
            t.abort();
            info!("CLOB REST task stopped");
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

async fn run_clob_ws(token_map: Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>) -> anyhow::Result<()> {
    // Connect to live RTDS websocket
    loop {
        debug!("Attempting RTDS WS connect to wss://ws-live-data.polymarket.com");
        match connect_async("wss://ws-live-data.polymarket.com").await {
            Ok((ws_stream, _resp)) => {
                info!("Connected to Polymarket RTDS WS");
                let (mut write, mut read) = ws_stream.split();

                // Subscribe to crypto_prices and Chainlink price updates
                let sub = serde_json::json!({
                    "action": "subscribe",
                    "subscriptions": [
                        {"topic": "crypto_prices", "type": "update"},
                        {"topic": "crypto_prices_chainlink", "type": "update"}
                    ]
                });
                let sub_str = sub.to_string();
                info!("Sending WS subscribe for topics: crypto_prices, crypto_prices_chainlink");
                if let Err(e) = write.send(Message::Text(sub_str.clone())).await {
                    error!("Failed to send subscribe: {}", e);
                }

                // Move write into a ping task (we only need send pings from here on)
                let ping_handle = tokio::spawn(async move {
                    let mut sink = write; // take ownership
                    loop {
                        let ping = serde_json::json!({"action":"ping"}).to_string();
                        if let Err(e) = sink.send(Message::Text(ping)).await {
                            debug!("WS ping failed: {}", e);
                            break;
                        }
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                });

                // One-time preview of the first incoming text message to help diagnose message shapes
                let mut previewed = false;

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(txt)) => {
                            if !previewed {
                                let short = txt.chars().take(300).collect::<String>();
                                debug!("WS sample text (first seen): {}", short);
                                previewed = true;
                            }

                            if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                // Detect and log subscription acknowledgements (one-liners)
                                let is_ack = v.get("type").and_then(|t| t.as_str()).map(|s| s.eq_ignore_ascii_case("subscribed")).unwrap_or(false)
                                    || (v.get("subscriptions").is_some() && v.get("data").is_none() && v.get("updates").is_none());
                                if is_ack {
                                    let short = serde_json::to_string(&v).unwrap_or_default().chars().take(200).collect::<String>();
                                    debug!("WS subscription ack: {}", short);
                                }

                                let updates = handle_ws_price_message(&v, &token_map);
                                if updates == 0 {
                                    // Short preview of structure to help diagnose mismatching shapes without spamming logs
                                    let keys: Vec<String> = v.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();
                                    let preview = serde_json::to_string(&v).unwrap_or_default();
                                    let short = preview.chars().take(200).collect::<String>();
                                    debug!("WS message had no matches; top_keys={:?} preview='{}'", keys, short);
                                }
                            } else {
                                debug!("WS message non-json: {}", txt);
                            }
                        }
                        Ok(Message::Ping(_)) => {
                            // ignore
                        }
                        Ok(_) => {}
                        Err(e) => {
                            error!("WS read error: {}", e);
                            break;
                        }
                    }
                }

                // If read loop ends, cancel ping and retry connect
                ping_handle.abort();
                debug!("WS read loop ended, reconnecting...");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
            Err(e) => {
                error!("RTDS WS connect error: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    }
}

fn handle_ws_price_message(v: &Value, token_map: &Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>) -> usize {
    let ts_ms = crate::models::now_ms();

    // Patterns to look for: data array, updates array, or direct object
    let mut candidates = Vec::new();

    if let Some(data) = v.get("data") {
        if data.is_array() {
            if let Some(arr) = data.as_array() {
                for item in arr { candidates.push(item.clone()); }
            }
        } else {
            candidates.push(data.clone());
        }
    }
    if let Some(updates) = v.get("updates") {
        if let Some(arr) = updates.as_array() {
            for item in arr { candidates.push(item.clone()); }
        }
    }

    // If nothing collected, maybe the message itself is a price object
    if candidates.is_empty() {
        candidates.push(v.clone());
    }

    let mut matched: usize = 0;

    for entry in candidates {
        // Try token id
        let token_id = entry.get("token_id")
            .or_else(|| entry.get("token"))
            .or_else(|| entry.get("asset_id"))
            .and_then(|t| t.as_str())
            .map(|s| s.to_string());

        if token_id.is_none() { continue; }
        let tid = token_id.unwrap();

        // Extract bid / ask if available
        let best_bid = entry.get("best_bid").or_else(|| entry.get("bid")).or_else(|| entry.get("bestBid"));
        let best_ask = entry.get("best_ask").or_else(|| entry.get("ask")).or_else(|| entry.get("bestAsk"));

        let parse_decimal = |v: &Value| -> Option<Decimal> {
            match v {
                Value::String(s) => Decimal::from_str(s).ok(),
                Value::Number(n) => Decimal::from_str(&n.to_string()).ok(),
                _ => None,
            }
        };

        let mut bid_dec: Option<Decimal> = None;
        let mut ask_dec: Option<Decimal> = None;

        if let Some(bv) = best_bid { bid_dec = parse_decimal(bv); }
        if let Some(av) = best_ask { ask_dec = parse_decimal(av); }

        // If single price + side provided
        if (bid_dec.is_none() && ask_dec.is_none()) && entry.get("price").is_some() {
            let pv = entry.get("price").unwrap();
            if let Some(p) = parse_decimal(pv) {
                if let Some(side_v) = entry.get("side").and_then(|s| s.as_str()) {
                    if side_v.eq_ignore_ascii_case("BUY") {
                        ask_dec = Some(p);
                    } else if side_v.eq_ignore_ascii_case("SELL") {
                        bid_dec = Some(p);
                    } else {
                        // unknown side - use as both
                        bid_dec = Some(p);
                        ask_dec = Some(p);
                    }
                } else {
                    bid_dec = Some(p);
                    ask_dec = Some(p);
                }
            }
        }

        // Apply to token map
        let map = token_map.read();
        if let Some((pair, side)) = map.get(&tid) {
            matched += 1;
            let mut p = pair.write();
            if let Some(b) = bid_dec {
                match side {
                    Side::Up => p.ws_up_bid = Some(b),
                    Side::Down => p.ws_down_bid = Some(b),
                }
                p.last_ws_update_ms = ts_ms;
            }
            if let Some(a) = ask_dec {
                match side {
                    Side::Up => p.ws_up_ask = Some(a),
                    Side::Down => p.ws_down_ask = Some(a),
                }
                p.last_ws_update_ms = ts_ms;
            }
            debug!("CLOB WS update {} side={:?} bid={:?} ask={:?}", tid, side, bid_dec, ask_dec);
        }
    }

    matched
}

// REST poller (runs in parallel for comparison/observability)
async fn run_clob_rest_loop(token_map: Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>) {
    let client = reqwest::Client::new();
    loop {
        if let Err(e) = run_clob_rest_once(&client, &token_map).await {
            error!("CLOB REST error: {}", e);
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn run_clob_rest_once(
    client: &reqwest::Client,
    token_map: &Arc<RwLock<HashMap<String, (Arc<RwLock<TradingPair>>, Side)>>>,
) -> anyhow::Result<()> {
    let token_ids: Vec<String> = token_map.read().keys().cloned().collect();

    if token_ids.is_empty() {
        debug!("No tokens to REST-poll, waiting 1s...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        return Ok(());
    }

    for tid in token_ids {
        if let Ok(Some((bid, ask))) = fetch_token_price(client, &tid).await {
            let map = token_map.read();
            if let Some((pair, side)) = map.get(&tid) {
                let mut p = pair.write();
                match side {
                    Side::Up => { p.rest_up_bid = Some(bid); p.rest_up_ask = Some(ask); }
                    Side::Down => { p.rest_down_bid = Some(bid); p.rest_down_ask = Some(ask); }
                }
                p.last_rest_update_ms = crate::models::now_ms();

                // Lower log level for frequent REST updates.
                debug!("CLOB REST update {} side={:?} bid={} ask={}", tid, side, bid, ask);
            }
        }
    }

    Ok(())
}

async fn fetch_token_price(
    client: &reqwest::Client,
    token_id: &str,
) -> anyhow::Result<Option<(Decimal, Decimal)>> {
    use std::str::FromStr;

    let host = crate::config::POLYMARKET_CLOB_HOST;

    // Use the legacy /price endpoint which returns {"price": <number>} for BUY/SELL sides when an orderbook exists.
    let buy_url = format!("{}/price?token_id={}&side=BUY", host, token_id);
    let sell_url = format!("{}/price?token_id={}&side=SELL", host, token_id);

    let mut buy_price: Option<Decimal> = None;
    let mut sell_price: Option<Decimal> = None;

    for (url, which) in vec![(buy_url, "BUY"), (sell_url, "SELL")] {
        debug!("CLOB REST GET {}", url);
        match client.get(&url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    // Do not spam logs for 404s; keep it debug-level noise.
                    debug!("Non-success response for {}: {}", url, resp.status());
                    continue;
                }
                let v: Value = resp.json().await?;
                if let Some(pv) = v.get("price") {
                    let pdec = match pv {
                        Value::String(s) => s.parse::<Decimal>().ok(),
                        Value::Number(n) => n.to_string().parse::<Decimal>().ok(),
                        _ => None,
                    };
                    if let Some(d) = pdec {
                        match which {
                            "BUY" => buy_price = Some(d),
                            "SELL" => sell_price = Some(d),
                            _ => {}
                        }
                        debug!("CLOB REST price endpoint {} side={} price={}", token_id, which, d);
                    } else {
                        debug!("CLOB REST {} returned price field but could not parse it: {}", url, pv);
                    }
                } else {
                    debug!("CLOB REST {} returned JSON without 'price' field", url);
                }
            }
            Err(e) => {
                debug!("CLOB REST fetch error for {}: {}", url, e);
                continue;
            }
        }
    }

    if buy_price.is_some() || sell_price.is_some() {
        let ask = buy_price.or(sell_price);
        let bid = sell_price.or(buy_price);
        if let (Some(bd), Some(ad)) = (bid, ask) {
            return Ok(Some((bd, ad)));
        } else if let Some(single) = ask.or(bid) {
            return Ok(Some((single, single)));
        }
    }

    Ok(None)
}