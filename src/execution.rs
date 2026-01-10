use std::str::FromStr as _;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::sync::{mpsc::UnboundedSender, Mutex, RwLock};
use tokio::time::{sleep, Instant};
use tracing::{debug, info, warn};

use futures::StreamExt;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer as _;
use polymarket_client_sdk::auth::state::{Authenticated, Unauthenticated};
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::request::{OrderBookSummaryRequest, OrdersRequest};
use polymarket_client_sdk::clob::types::{Amount, Side, SignatureType};
use polymarket_client_sdk::clob::types::response::PostOrderResponse;
use polymarket_client_sdk::clob::ws::types::response::{OrderMessage, TradeMessage};
use polymarket_client_sdk::clob::ws::Client as WsClient;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal};
use polymarket_client_sdk::POLYGON;
use std::collections::VecDeque;

use crate::config::BotConfig;

pub struct ExecutionEngine {
    client: ClobClient<
        polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
    >,
    signer: PrivateKeySigner,
    api_key: polymarket_client_sdk::auth::ApiKey,
    rate_limiter: OrderRateLimiter,
    ws_client: WsClient<Unauthenticated>,
    orderbooks: std::sync::Arc<RwLock<std::collections::HashMap<String, BookSnapshot>>>,
    price_history: std::sync::Arc<RwLock<std::collections::HashMap<String, VecDeque<Decimal>>>>,
    ws_state: Mutex<WsState>,
    ws_user_client: WsClient<Authenticated<Normal>>,
    user_state: Mutex<UserWsState>,
    user_stats: std::sync::Arc<RwLock<UserStats>>,
    open_orders: std::sync::Arc<RwLock<std::collections::HashMap<String, OpenOrder>>>,
    token_meta: std::sync::Arc<RwLock<std::collections::HashMap<String, TokenMeta>>>,
    inventory_update_tx: Option<UnboundedSender<String>>,
    book_update_tx: Option<UnboundedSender<String>>,
}

#[derive(Debug, Clone)]
pub struct BookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct TokenMeta {
    pub tick_size: Decimal,
    pub min_order_size: Decimal,
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

#[derive(Debug, Clone)]
struct OpenOrder {
    asset_id: String,
    price: Decimal,
    remaining: Decimal,
    side: Side,
}

#[derive(Debug, Clone)]
pub struct OpenOrderView {
    pub id: String,
    pub token_id: String,
    pub price: Decimal,
    pub remaining: Decimal,
    pub side: Side,
}

#[derive(Debug, Clone)]
pub struct LimitOrderRequest {
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
}

impl ExecutionEngine {
    pub async fn new(
        config: &BotConfig,
        inventory_update_tx: Option<UnboundedSender<String>>,
        book_update_tx: Option<UnboundedSender<String>>,
    ) -> Result<Self> {
        let signer = PrivateKeySigner::from_str(&config.private_key)
            .context("Invalid POLY_PRIVATE_KEY")?
            .with_chain_id(Some(POLYGON));

        let funder =
            Address::from_str(&config.proxy_address).context("Invalid POLY_PROXY_ADDRESS")?;

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

        let ws_user_client =
            WsClient::default().authenticate(credentials.clone(), signer.address())?;

        let rate_limiter = OrderRateLimiter::new(config.max_orders_per_second);

        Ok(Self {
            client,
            signer,
            api_key: credentials.key(),
            rate_limiter,
            ws_client: WsClient::default(),
            orderbooks: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
            price_history: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
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
            open_orders: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
            token_meta: std::sync::Arc::new(RwLock::new(std::collections::HashMap::new())),
            inventory_update_tx,
            book_update_tx,
        })
    }

    pub fn client(
        &self,
    ) -> &ClobClient<
        polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
    > {
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
        let books = self.orderbooks.clone();
        let history = self.price_history.clone();
        let token_ids = token_ids.clone();
        let book_tx = self.book_update_tx.clone();

        state.task = Some(tokio::spawn(async move {
            let stream = match ws_client.subscribe_orderbook(token_ids) {
                Ok(stream) => stream,
                Err(err) => {
                    debug!(error = %err, "Failed to subscribe to orderbook");
                    return;
                }
            };
            let mut stream = Box::pin(stream);
            while let Some(update) = stream.next().await {
                match update {
                    Ok(book) => {
                        let mut cache = books.write().await;
                        let mut bids: Vec<BookLevel> = book
                            .bids
                            .iter()
                            .map(|level| BookLevel {
                                price: level.price,
                                size: level.size,
                            })
                            .collect();
                        bids.sort_by(|a, b| b.price.cmp(&a.price));
                        bids.truncate(5);

                        let mut asks: Vec<BookLevel> = book
                            .asks
                            .iter()
                            .map(|level| BookLevel {
                                price: level.price,
                                size: level.size,
                            })
                            .collect();
                        asks.sort_by(|a, b| a.price.cmp(&b.price));
                        asks.truncate(5);
                        cache.insert(
                            book.asset_id.clone(),
                            BookSnapshot {
                                bids,
                                asks,
                                updated_at: book.timestamp,
                            },
                        );
                        if let Some(best_bid) = cache
                            .get(&book.asset_id)
                            .and_then(|snap| snap.bids.first().map(|level| level.price))
                        {
                            let mut history = history.write().await;
                            let entry = history
                                .entry(book.asset_id.clone())
                                .or_insert_with(VecDeque::new);
                            entry.push_back(best_bid);
                            while entry.len() > 5 {
                                entry.pop_front();
                            }
                        }
                        if let Some(tx) = &book_tx {
                            let _ = tx.send(book.asset_id.clone());
                        }
                    }
                    Err(err) => {
                        debug!(error = %err, "Orderbook stream error");
                    }
                }
            }
        }));

        Ok(())
    }

    pub async fn cached_book(&self, token_id: &str) -> Option<BookSnapshot> {
        let cache = self.orderbooks.read().await;
        cache.get(token_id).cloned()
    }

    pub async fn momentum(&self, token_id: &str) -> Option<Decimal> {
        let history = self.price_history.read().await;
        let series = history.get(token_id)?;
        if series.len() < 2 {
            return None;
        }
        let first = series.front()?;
        let last = series.back()?;
        Some(*last - *first)
    }

    pub async fn token_meta(&self, token_id: &str) -> Result<TokenMeta> {
        if let Some(meta) = self.token_meta.read().await.get(token_id).cloned() {
            return Ok(meta);
        }
        let request = OrderBookSummaryRequest::builder()
            .token_id(token_id)
            .build();
        let response = self.client.order_book(&request).await?;
        let meta = TokenMeta {
            tick_size: Decimal::from(response.tick_size),
            min_order_size: response.min_order_size,
        };
        self.token_meta
            .write()
            .await
            .insert(token_id.to_string(), meta.clone());
        Ok(meta)
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
        let open_orders = self.open_orders.clone();
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
                            Some(Ok(msg)) => handle_order_message(
                                &stats,
                                open_orders.clone(),
                                api_key,
                                inventory_tx.clone(),
                                msg,
                            ).await,
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

    pub async fn pending_size(&self, token_id: &str) -> Decimal {
        let cache = self.open_orders.read().await;
        cache
            .values()
            .filter(|order| order.asset_id == token_id)
            .fold(Decimal::ZERO, |acc, order| acc + order.remaining)
    }

    pub async fn open_order_count(&self, token_ids: &[&str]) -> usize {
        let cache = self.open_orders.read().await;
        cache
            .values()
            .filter(|order| token_ids.iter().any(|id| *id == order.asset_id))
            .count()
    }

    pub async fn open_orders_for_tokens(&self, token_ids: &[&str]) -> Vec<OpenOrderView> {
        let cache = self.open_orders.read().await;
        cache
            .iter()
            .filter_map(|(id, order)| {
                if !token_ids.iter().any(|token| *token == order.asset_id) {
                    return None;
                }
                if order.side != Side::Buy {
                    return None;
                }
                Some(OpenOrderView {
                    id: id.clone(),
                    token_id: order.asset_id.clone(),
                    price: order.price,
                    remaining: order.remaining,
                    side: order.side,
                })
            })
            .collect()
    }

    pub fn notify_inventory_changed(&self, market_id: String) {
        if let Some(tx) = &self.inventory_update_tx {
            let _ = tx.send(market_id);
        }
    }

    pub async fn order_book(
        &self,
        token_id: &str,
    ) -> Result<polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse> {
        let request = OrderBookSummaryRequest::builder()
            .token_id(token_id)
            .build();
        Ok(self.client.order_book(&request).await?)
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

        debug!(order_id = %response.order_id, "Order accepted");

        Ok(response.order_id)
    }

    pub async fn place_limit_orders(
        &self,
        orders: Vec<LimitOrderRequest>,
    ) -> Result<Vec<PostOrderResponse>> {
        let mut signed_orders = Vec::with_capacity(orders.len());
        for order in orders {
            self.rate_limiter.wait().await;
            let signable = self
                .client
                .limit_order()
                .token_id(&order.token_id)
                .side(Side::Buy)
                .price(order.price)
                .size(order.size)
                .build()
                .await
                .with_context(|| {
                    format!(
                        "Failed to build limit order: price={} size={}",
                        order.price, order.size
                    )
                })?;

            let signed = self
                .client
                .sign(&self.signer, signable)
                .await
                .context("Failed to sign order")?;

            signed_orders.push(signed);
        }

        self.client
            .post_orders(signed_orders)
            .await
            .context("Failed to submit batch orders")
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
                        debug!(order_id = %order.id, "Cancelled open order");
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

    pub async fn cancel_orders(&self, order_ids: &[String]) -> Result<usize> {
        let mut cancelled = 0usize;
        for order_id in order_ids {
            self.rate_limiter.wait().await;
            match self.client.cancel_order(order_id).await {
                Ok(_) => {
                    cancelled += 1;
                    debug!(order_id = %order_id, "Cancelled open order");
                }
                Err(err) => {
                    warn!(order_id = %order_id, error = %err, "Failed to cancel order");
                }
            }
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
    open_orders: std::sync::Arc<RwLock<std::collections::HashMap<String, OpenOrder>>>,
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
        msg_type = ?msg.msg_type,
        "User order update"
    );

    track_open_order(&open_orders, &msg).await;

    if let Some(tx) = inventory_tx {
        let _ = tx.send(msg.market.clone());
    }
}

async fn track_open_order(
    open_orders: &std::sync::Arc<RwLock<std::collections::HashMap<String, OpenOrder>>>,
    msg: &OrderMessage,
) {
    let msg_type = msg.msg_type.as_deref().unwrap_or("");
    let mut cache = open_orders.write().await;

    if msg_type.eq_ignore_ascii_case("CANCELLATION") {
        cache.remove(&msg.id);
        return;
    }

    let Some(original_size) = msg.original_size else {
        return;
    };
    let matched = msg.size_matched.unwrap_or(Decimal::ZERO);
    let remaining = (original_size - matched).max(Decimal::ZERO);

    if remaining.is_zero() {
        cache.remove(&msg.id);
        return;
    }

    cache.insert(
        msg.id.clone(),
        OpenOrder {
            asset_id: msg.asset_id.clone(),
            price: msg.price,
            remaining,
            side: msg.side,
        },
    );
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
    debug!(
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
