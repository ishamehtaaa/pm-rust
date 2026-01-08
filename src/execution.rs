use std::str::FromStr as _;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc::UnboundedSender, Mutex, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use futures::StreamExt;

use alloy::signers::Signer as _;
use alloy::signers::local::PrivateKeySigner;
use polymarket_client_sdk::clob::types::request::{
    MidpointRequest, OrderBookSummaryRequest, OrdersRequest, PriceRequest,
};
use polymarket_client_sdk::clob::types::{Amount, Side, SignatureType};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::ws::types::response::{OrderMessage, TradeMessage};
use polymarket_client_sdk::auth::state::{Authenticated, Unauthenticated};
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::types::{Address, Decimal};
use polymarket_client_sdk::POLYGON;

use crate::config::BotConfig;

pub struct ExecutionEngine {
    client: ClobClient<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>>,
    signer: PrivateKeySigner,
    api_key: polymarket_client_sdk::auth::ApiKey,
    rate_limiter: OrderRateLimiter,
    ws_client: WsClient<Unauthenticated>,
    best_prices: std::sync::Arc<RwLock<std::collections::HashMap<String, BestPrice>>>,
    ws_state: Mutex<WsState>,
    ws_user_client: WsClient<Authenticated<Normal>>,
    user_state: Mutex<UserWsState>,
    user_stats: std::sync::Arc<RwLock<UserStats>>,
    inventory_update_tx: Option<UnboundedSender<String>>,
}

#[derive(Debug, Clone)]
pub struct BestPrice {
    pub bid: Decimal,
    pub ask: Decimal,
    pub updated_at: i64,
}

struct WsState {
    tokens: std::collections::HashSet<String>,
    task: Option<tokio::task::JoinHandle<()>>,
}

struct UserWsState {
    markets: std::collections::HashSet<String>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Default, Clone)]
pub struct UserStats {
    pub orders_seen: u64,
    pub trades_seen: u64,
    pub last_trade_market: Option<String>,
    pub last_trade_size: Option<Decimal>,
    pub matched_size_total: Decimal,
}

impl ExecutionEngine {
    pub async fn new(
        config: &BotConfig,
        inventory_update_tx: Option<UnboundedSender<String>>,
    ) -> Result<Self> {
        let signer = PrivateKeySigner::from_str(&config.private_key)
            .context("Invalid POLY_PRIVATE_KEY")?
            .with_chain_id(Some(POLYGON));

        let funder = Address::from_str(&config.proxy_address)
            .context("Invalid POLY_PROXY_ADDRESS")?;

        let client = ClobClient::new(&config.clob_host, ClobConfig::default())?
            .authentication_builder(&signer)
            .funder(funder)
            .signature_type(SignatureType::Proxy)
            .authenticate()
            .await?;

        let unauth_client = ClobClient::new(&config.clob_host, ClobConfig::default())?;
        let credentials = match unauth_client.derive_api_key(&signer, None).await {
            Ok(creds) => creds,
            Err(_) => unauth_client.create_api_key(&signer, None).await?,
        };

        let ws_user_client = WsClient::default().authenticate(credentials.clone(), signer.address())?;

        let rate_limiter = OrderRateLimiter::new(config.max_orders_per_second);

        Ok(Self {
            client,
            signer,
            api_key: credentials.key(),
            rate_limiter,
            ws_client: WsClient::default(),
            best_prices: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
            ws_state: Mutex::new(WsState {
                tokens: std::collections::HashSet::new(),
                task: None,
            }),
            ws_user_client,
            user_state: Mutex::new(UserWsState {
                markets: std::collections::HashSet::new(),
                task: None,
            }),
            user_stats: std::sync::Arc::new(RwLock::new(UserStats::default())),
            inventory_update_tx,
        })
    }

    pub fn client(&self) -> &ClobClient<polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>> {
        &self.client
    }

    pub async fn update_ws_tokens(&self, token_ids: Vec<String>) -> Result<()> {
        let mut state = self.ws_state.lock().await;
        let new_set: std::collections::HashSet<String> = token_ids.iter().cloned().collect();
        if state.tokens == new_set {
            return Ok(());
        }

        if let Some(task) = state.task.take() {
            task.abort();
        }

        state.tokens = new_set.clone();

        let ws_client = self.ws_client.clone();
        let prices = self.best_prices.clone();
        let token_ids = token_ids.clone();

        state.task = Some(tokio::spawn(async move {
            let stream = match ws_client.subscribe_best_bid_ask(token_ids) {
                Ok(stream) => stream,
                Err(err) => {
                    debug!(error = %err, "Failed to subscribe to best bid/ask");
                    return;
                }
            };
            let mut stream = Box::pin(stream);
            while let Some(update) = stream.next().await {
                match update {
                    Ok(bba) => {
                        let mut cache = prices.write().await;
                        cache.insert(
                            bba.asset_id.clone(),
                            BestPrice {
                                bid: bba.best_bid,
                                ask: bba.best_ask,
                                updated_at: bba.timestamp,
                            },
                        );
                    }
                    Err(err) => {
                        debug!(error = %err, "Best bid/ask stream error");
                    }
                }
            }
        }));

        Ok(())
    }

    pub async fn cached_best_prices(&self, token_id: &str) -> Option<(Decimal, Decimal)> {
        let cache = self.best_prices.read().await;
        cache.get(token_id).map(|price| (price.bid, price.ask))
    }

    pub async fn update_user_ws_markets(&self, market_ids: Vec<String>) -> Result<()> {
        let mut state = self.user_state.lock().await;
        let new_set: std::collections::HashSet<String> = market_ids.iter().cloned().collect();
        if state.markets == new_set {
            return Ok(());
        }

        if let Some(task) = state.task.take() {
            task.abort();
        }

        state.markets = new_set.clone();

        let ws_user_client = self.ws_user_client.clone();
        let markets = market_ids.clone();
        let stats = self.user_stats.clone();
        let api_key = self.api_key;
        let inventory_tx = self.inventory_update_tx.clone();

        state.task = Some(tokio::spawn(async move {
            let orders_stream = match ws_user_client.subscribe_orders(markets.clone()) {
                Ok(stream) => stream,
                Err(err) => {
                    debug!(error = %err, "Failed to subscribe to user orders");
                    return;
                }
            };

            let trades_stream = match ws_user_client.subscribe_trades(markets) {
                Ok(stream) => stream,
                Err(err) => {
                    debug!(error = %err, "Failed to subscribe to user trades");
                    return;
                }
            };

            let mut orders_stream = Box::pin(orders_stream);
            let mut trades_stream = Box::pin(trades_stream);

            loop {
                tokio::select! {
                    order = orders_stream.next() => {
                        match order {
                            Some(Ok(msg)) => handle_order_message(&stats, api_key, inventory_tx.clone(), msg).await,
                            Some(Err(err)) => debug!(error = %err, "User order stream error"),
                            None => break,
                        }
                    }
                    trade = trades_stream.next() => {
                        match trade {
                            Some(Ok(msg)) => handle_trade_message(&stats, api_key, inventory_tx.clone(), msg).await,
                            Some(Err(err)) => debug!(error = %err, "User trade stream error"),
                            None => break,
                        }
                    }
                }
            }
        }));

        Ok(())
    }

    pub async fn user_stats(&self) -> UserStats {
        self.user_stats.read().await.clone()
    }

    pub fn notify_inventory_changed(&self, market_id: String) {
        if let Some(tx) = &self.inventory_update_tx {
            let _ = tx.send(market_id);
        }
    }

    pub async fn order_book(&self, token_id: &str) -> Result<polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse> {
        let request = OrderBookSummaryRequest::builder()
            .token_id(token_id)
            .build();
        Ok(self.client.order_book(&request).await?)
    }

    pub async fn price(&self, token_id: &str, side: Side) -> Result<Decimal> {
        let request = PriceRequest::builder()
            .token_id(token_id)
            .side(side)
            .build();
        Ok(self.client.price(&request).await?.price)
    }

    pub async fn midpoint(&self, token_id: &str) -> Result<Decimal> {
        let request = MidpointRequest::builder().token_id(token_id).build();
        Ok(self.client.midpoint(&request).await?.mid)
    }

    pub async fn place_limit_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
    ) -> Result<String> {
        self.rate_limiter.wait().await;

        debug!(
            token_id = %token_id,
            price = %price,
            size = %size,
            "Submitting limit order"
        );

        let signable = self
            .client
            .limit_order()
            .token_id(token_id)
            .side(Side::Buy)
            .price(price)
            .size(size)
            .build()
            .await
            .with_context(|| format!("Failed to build limit order: price={price} size={size}"))?;

        let signed = self
            .client
            .sign(&self.signer, signable)
            .await
            .context("Failed to sign order")?;

        let response = self
            .client
            .post_order(signed)
            .await
            .context("Failed to submit order")?;

        if !response.success {
            return Err(anyhow!(
                "Order rejected: {:?}",
                response.error_msg.unwrap_or_else(|| "unknown".to_string())
            ));
        }

        info!(order_id = %response.order_id, "Order accepted");

        Ok(response.order_id)
    }

    pub async fn place_market_order_usdc(
        &self,
        token_id: &str,
        amount_usdc: Decimal,
    ) -> Result<String> {
        self.rate_limiter.wait().await;

        debug!(
            token_id = %token_id,
            amount_usdc = %amount_usdc,
            "Submitting market order"
        );

        let amount = Amount::usdc(amount_usdc).context("Invalid USDC amount")?;
        let signable = self
            .client
            .market_order()
            .token_id(token_id)
            .side(Side::Buy)
            .amount(amount)
            .build()
            .await
            .context("Failed to build market order")?;

        let signed = self
            .client
            .sign(&self.signer, signable)
            .await
            .context("Failed to sign market order")?;

        let response = self
            .client
            .post_order(signed)
            .await
            .context("Failed to submit market order")?;

        if !response.success {
            return Err(anyhow!(
                "Market order rejected: {:?}",
                response.error_msg.unwrap_or_else(|| "unknown".to_string())
            ));
        }

        info!(order_id = %response.order_id, "Market order accepted");
        Ok(response.order_id)
    }

    pub async fn cancel_orders_for_token(&self, token_id: &str) -> Result<usize> {
        let mut cancelled = 0usize;
        let mut next_cursor: Option<String> = None;

        loop {
            let page = self
                .client
                .orders(
                    &OrdersRequest::builder().asset_id(token_id).build(),
                    next_cursor.clone(),
                )
                .await?;

            for order in &page.data {
                self.rate_limiter.wait().await;
                match self.client.cancel_order(&order.id).await {
                    Ok(_) => {
                        cancelled += 1;
                        info!(order_id = %order.id, "Cancelled open order");
                    }
                    Err(err) => {
                        warn!(order_id = %order.id, error = %err, "Failed to cancel order");
                    }
                }
            }

            if page.next_cursor == "LTE=" {
                break;
            }
            next_cursor = Some(page.next_cursor);
        }

        Ok(cancelled)
    }
}

struct OrderRateLimiter {
    min_interval: Duration,
    last_sent: Mutex<Instant>,
}

async fn handle_order_message(
    stats: &std::sync::Arc<RwLock<UserStats>>,
    api_key: polymarket_client_sdk::auth::ApiKey,
    inventory_tx: Option<UnboundedSender<String>>,
    msg: OrderMessage,
) {
    if msg.owner != Some(api_key) && msg.order_owner != Some(api_key) {
        return;
    }
    let mut stats = stats.write().await;
    stats.orders_seen += 1;
    debug!(
        order_id = %msg.id,
        market = %msg.market,
        side = ?msg.side,
        price = %msg.price,
        size = ?msg.original_size,
        matched = ?msg.size_matched,
        "User order update"
    );

    if let Some(tx) = inventory_tx {
        let _ = tx.send(msg.market.clone());
    }
}

async fn handle_trade_message(
    stats: &std::sync::Arc<RwLock<UserStats>>,
    api_key: polymarket_client_sdk::auth::ApiKey,
    inventory_tx: Option<UnboundedSender<String>>,
    msg: TradeMessage,
) {
    if msg.owner != Some(api_key) && msg.trade_owner != Some(api_key) {
        return;
    }
    let mut stats = stats.write().await;
    stats.trades_seen += 1;
    stats.last_trade_market = Some(msg.market.clone());
    stats.last_trade_size = Some(msg.size);
    stats.matched_size_total += msg.size;
    info!(
        market = %msg.market,
        side = ?msg.side,
        size = %msg.size,
        price = %msg.price,
        status = %msg.status,
        "User trade update"
    );

    if let Some(tx) = inventory_tx {
        let _ = tx.send(msg.market.clone());
    }
}

impl OrderRateLimiter {
    fn new(max_per_second: u32) -> Self {
        let interval = if max_per_second == 0 {
            Duration::from_secs(1)
        } else {
            Duration::from_secs_f64(1.0 / max_per_second as f64)
        };
        Self {
            min_interval: interval,
            last_sent: Mutex::new(Instant::now() - interval),
        }
    }

    async fn wait(&self) {
        let mut last = self.last_sent.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_interval {
            let sleep_for = self.min_interval - elapsed;
            debug!(?sleep_for, "Rate limiting order submission");
            sleep(sleep_for).await;
        }
        *last = Instant::now();
    }
}
