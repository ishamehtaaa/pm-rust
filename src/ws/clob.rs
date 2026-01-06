use crate::models::Side;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use futures_util::StreamExt;
use polymarket_client_sdk::clob::ws::Client as WsClient;
use serde_json::Value;

pub struct ClobFeed {
    // token_id -> Side
    token_map: Arc<RwLock<HashMap<String, Side>>>,
    // Internal price storages (independent of TradingPair)
    ws_prices: Arc<RwLock<HashMap<String, (Option<Decimal>, Option<Decimal>)>>>,
    rest_prices: Arc<RwLock<HashMap<String, (Option<Decimal>, Option<Decimal>)>>>,
    ws_task: Option<JoinHandle<()>>,
    rest_task: Option<JoinHandle<()>>,
} 

impl ClobFeed {
    pub fn new() -> Self {
        Self {
            token_map: Arc::new(RwLock::new(HashMap::new())),
            ws_prices: Arc::new(RwLock::new(HashMap::new())),
            rest_prices: Arc::new(RwLock::new(HashMap::new())),
            ws_task: None,
            rest_task: None,
        }
    }

    /// Replace tracked tokens. This accepts (token_id, Side) pairs and will only track token IDs/sides.
    pub fn set_pairs(&mut self, tokens: Vec<(String, Side)>) {
        let mut map = HashMap::new();

        for (token_id, side) in tokens {
            map.insert(token_id.clone(), side);
            debug!("Registered token {} side={:?}", token_id, side);
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
        let ws_prices = self.ws_prices.clone();
        let ws_handle = tokio::spawn(async move {
            if let Err(e) = run_clob_ws(token_map_ws, ws_prices).await {
                error!("CLOB WS task terminated: {}", e);
            }
        });

        let token_map_rest = self.token_map.clone();
        let rest_prices = self.rest_prices.clone();
        let rest_handle = tokio::spawn(async move {
            run_clob_rest_loop(token_map_rest, rest_prices).await;
        });

        self.ws_task = Some(ws_handle);
        self.rest_task = Some(rest_handle);
        info!("CLOB feed started (WS + REST poller)");
    }

    /// Start WS only (lowest latency).
    pub fn start_ws_only(&mut self) {
        if self.ws_task.is_some() || self.rest_task.is_some() {
            return;
        }

        let token_map_ws = self.token_map.clone();
        let ws_prices = self.ws_prices.clone();
        let ws_handle = tokio::spawn(async move {
            if let Err(e) = run_clob_ws(token_map_ws, ws_prices).await {
                error!("CLOB WS task terminated: {}", e);
            }
        });

        self.ws_task = Some(ws_handle);
        info!("CLOB feed started (WS only)");
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

    pub fn ws_price(&self, token_id: &str) -> Option<(Option<Decimal>, Option<Decimal>)> {
        self.ws_prices.read().get(token_id).cloned()
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

async fn run_clob_ws(
    token_map: Arc<RwLock<HashMap<String, Side>>>,
    ws_prices: Arc<RwLock<HashMap<String, (Option<Decimal>, Option<Decimal>)>>>,
) -> anyhow::Result<()> {
    loop {
        let asset_ids: Vec<String> = token_map.read().keys().cloned().collect();
        if asset_ids.is_empty() {
            debug!("No tokens to subscribe for WS orderbook, waiting 1s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        let client = WsClient::default();
        info!("Connected to Polymarket CLOB WS (orderbook)");
        let stream = match client.subscribe_orderbook(asset_ids.clone()) {
            Ok(s) => s,
            Err(e) => {
                error!("Orderbook subscribe failed: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue;
            }
        };

        let mut stream = Box::pin(stream);
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(book) => {
                    let bid = book
                        .bids
                        .iter()
                        .filter(|lvl| lvl.size > Decimal::ZERO)
                        .map(|lvl| lvl.price)
                        .max();
                    let ask = book
                        .asks
                        .iter()
                        .filter(|lvl| lvl.size > Decimal::ZERO)
                        .map(|lvl| lvl.price)
                        .min();
                    ws_prices.write().insert(book.asset_id.clone(), (bid, ask));
                    debug!(
                        "CLOB WS book update asset_id={} bid={:?} ask={:?}",
                        book.asset_id, bid, ask
                    );
                }
                Err(e) => {
                    error!("Orderbook stream error: {}", e);
                    break;
                }
            }
        }

        debug!("Orderbook stream ended, reconnecting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

// REST poller (runs in parallel for comparison/observability)
async fn run_clob_rest_loop(
    token_map: Arc<RwLock<HashMap<String, Side>>>,
    rest_prices: Arc<RwLock<HashMap<String, (Option<Decimal>, Option<Decimal>)>>>,
) {
    let client = reqwest::Client::new();
    loop {
        if let Err(e) = run_clob_rest_once(&client, &token_map, &rest_prices).await {
            error!("CLOB REST error: {}", e);
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}

async fn run_clob_rest_once(
    client: &reqwest::Client,
    token_map: &Arc<RwLock<HashMap<String, Side>>>,
    rest_prices: &Arc<RwLock<HashMap<String, (Option<Decimal>, Option<Decimal>)>>>,
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
            if let Some(_side) = map.get(&tid) {
                rest_prices.write().insert(tid.clone(), (Some(bid), Some(ask)));
                // Lower log level for frequent REST updates.
                debug!("CLOB REST update {} bid={} ask={}", tid, bid, ask);
            }
        }
    }

    Ok(())
}

pub(crate) async fn fetch_token_price(
    client: &reqwest::Client,
    token_id: &str,
) -> anyhow::Result<Option<(Decimal, Decimal)>> {
    

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
