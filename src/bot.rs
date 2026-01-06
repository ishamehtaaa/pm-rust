use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{MarketState, Side, TradingPair};

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::{OrderType, Side as ClobSide};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::data::{
    Client as DataClient,
    types::{request::PositionsRequest, response::Position},
};
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

// Configuration constants
const STATUS_PRINT_INTERVAL: Duration = Duration::from_secs(5);
const SCAN_LOOP_DELAY: Duration = Duration::from_millis(100);
const PRICE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const POSITIONS_POLL_INTERVAL: Duration = Duration::from_secs(2);
const LADDER_REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const DEFAULT_WINDOW_DURATION: Duration = Duration::from_secs(900);

// Order pricing constants
const TICK_SIZE_DP: u32 = 2; // Polymarket uses 0.01 tick size (2 decimal places)
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const LADDER_PRICE_STEP: Decimal = dec!(0.01);

/// Round price down to valid tick size for buy orders (conservative)
fn round_to_tick_buy(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::ToZero)
}

fn clamp_price(price: Decimal) -> Decimal {
    round_to_tick_buy(price).max(MIN_PRICE).min(MAX_PRICE)
}

#[derive(Debug, Clone, Default)]
struct MarketInventory {
    up_shares: Decimal,
    down_shares: Decimal,
    up_avg: Option<Decimal>,
    down_avg: Option<Decimal>,
}

impl MarketInventory {
    fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }

    fn avg_for_side(&self, side: Side) -> Option<Decimal> {
        match side {
            Side::Up => self.up_avg,
            Side::Down => self.down_avg,
        }
    }
}

#[derive(Debug, Clone)]
struct RestingOrder {
    order_id: String,
    price: Decimal,
    size: Decimal,
}

#[derive(Debug, Default)]
struct MarketOrders {
    up: Vec<RestingOrder>,
    down: Vec<RestingOrder>,
    last_refresh: Option<Instant>,
}

pub struct HighFreqArbBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    inventory: HashMap<String, MarketInventory>,
    orders: HashMap<String, MarketOrders>,
    token_to_market: HashMap<String, String>,
    token_to_side: HashMap<String, Side>,
    data_client: DataClient,
    user: Address,
    current_window_end: Option<DateTime<Utc>>,
    last_status_print: Instant,
    last_price_poll: Instant,
    last_positions_poll: Instant,
}

impl HighFreqArbBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing HighFreqArbBot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        debug!("Market cache initialized");

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| {
                error!(error = %e, "Failed to parse private key");
                e
            })?;
        debug!("Signer created successfully");

        info!("Authenticating with Polymarket CLOB");
        let client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())
            .map_err(|e| {
                error!(error = %e, "Failed to create CLOB client");
                e
            })?
            .authentication_builder(&signer.clone().with_chain_id(Some(POLYGON)))
            .authenticate()
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to authenticate with CLOB");
                e
            })?;
        info!("Successfully authenticated with Polymarket CLOB");

        let user = Address::from_str(&signer.address().to_string())?;

        Ok(Self {
            config,
            client: Arc::new(client),
            signer,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            inventory: HashMap::new(),
            orders: HashMap::new(),
            token_to_market: HashMap::new(),
            token_to_side: HashMap::new(),
            data_client: DataClient::default(),
            user,
            current_window_end: None,
            last_status_print: Instant::now(),
            last_price_poll: Instant::now(),
            last_positions_poll: Instant::now(),
        })
    }

    pub async fn run(&mut self) {
        info!(
            dry_run = self.config.dry_run,
            shares_per_side = %self.config.arb_config.shares_per_side,
            "Bot starting - single market per asset mode"
        );

        self.discover_markets().await;

        info!("Entering main loop");
        let mut loop_count: u64 = 0;

        loop {
            loop_count += 1;
            let now = Utc::now();

            if self.should_rotate_markets(now) {
                info!(
                    inventory_cleared = self.inventory.len(),
                    "Window expired, rotating markets"
                );
                self.inventory.clear();
                self.orders.clear();
                self.discover_markets().await;
                continue;
            }

            if self.last_price_poll.elapsed() > PRICE_POLL_INTERVAL {
                self.update_prices().await;
                self.last_price_poll = Instant::now();
            }

            if self.last_positions_poll.elapsed() > POSITIONS_POLL_INTERVAL {
                self.update_positions().await;
                self.last_positions_poll = Instant::now();
            }

            if self.last_status_print.elapsed() > STATUS_PRINT_INTERVAL {
                self.log_status();
                self.last_status_print = Instant::now();
            }

            self.maintain_all_ladders().await;

            if loop_count % 1000 == 0 {
                debug!(
                    loop_count,
                    markets = self.markets.len(),
                    inventory = self.inventory.len(),
                    "Main loop heartbeat"
                );
            }

            tokio::time::sleep(SCAN_LOOP_DELAY).await;
        }
    }

    async fn update_positions(&mut self) {
        let builder = PositionsRequest::builder().user(self.user.clone());
        let builder = match builder.limit(500) {
            Ok(b) => b,
            Err(e) => {
                warn!("Positions request invalid: {}", e);
                return;
            }
        };
        let request = builder.build();

        let positions = match self.data_client.positions(&request).await {
            Ok(p) => p,
            Err(e) => {
                warn!("Positions poll failed: {}", e);
                return;
            }
        };

        self.apply_positions_snapshot(positions);
    }

    fn apply_positions_snapshot(&mut self, positions: Vec<Position>) {
        let mut aggregated: HashMap<String, MarketInventory> = HashMap::new();
        let mut seen = std::collections::HashSet::new();

        for position in positions {
            let market_id = match self.token_to_market.get(position.asset.as_str()) {
                Some(m) => m.clone(),
                None => continue,
            };
            let side = match self.token_to_side.get(position.asset.as_str()) {
                Some(s) => *s,
                None => continue,
            };

            let entry = aggregated.entry(market_id.clone()).or_default();
            match side {
                Side::Up => {
                    entry.up_shares = position.size;
                    entry.up_avg = Some(position.avg_price);
                }
                Side::Down => {
                    entry.down_shares = position.size;
                    entry.down_avg = Some(position.avg_price);
                }
            }
            seen.insert(market_id);
        }

        for (market_id, inventory) in aggregated {
            self.inventory.insert(market_id.clone(), inventory);
        }

        for market_id in self.markets.keys() {
            if !seen.contains(market_id) {
                self.inventory.insert(market_id.clone(), MarketInventory::default());
            }
        }
    }

    async fn maintain_all_ladders(&mut self) {
        let ids: Vec<String> = self.markets.keys().cloned().collect();
        for id in ids {
            self.maintain_ladder(&id).await;
        }
    }

    async fn maintain_ladder(&mut self, market_id: &str) {
        let now = Instant::now();
        {
            let orders = self.orders.entry(market_id.to_string()).or_default();
            if orders
                .last_refresh
                .map(|t| t.elapsed() < LADDER_REFRESH_INTERVAL)
                .unwrap_or(false)
            {
                return;
            }
            orders.last_refresh = Some(now);
        }

        let (up_ask, down_ask) = {
            let state = match self.markets.get(market_id) {
                Some(s) => s,
                None => return,
            };
            let pair = state.pair.read();
            match (pair.latest_up_ask(), pair.latest_down_ask()) {
                (Some(u), Some(d)) => (u, d),
                _ => {
                    drop(pair);
                    self.cancel_all_orders(market_id).await;
                    return;
                }
            }
        };

        let inv = self.inventory.get(market_id).cloned().unwrap_or_default();
        let max_total_cost = self.config.legging_config.max_total_cost;
        let max_shares_per_side = self.config.legging_config.max_shares_per_side;
        let remaining_up = (max_shares_per_side - inv.up_shares).max(Decimal::ZERO);
        let remaining_down = (max_shares_per_side - inv.down_shares).max(Decimal::ZERO);

        if let (Some(up_avg), Some(down_avg)) = (inv.up_avg, inv.down_avg) {
            if up_avg + down_avg > max_total_cost {
                self.cancel_all_orders(market_id).await;
                return;
            }
        }

        if remaining_up <= Decimal::ZERO && remaining_down <= Decimal::ZERO {
            self.cancel_all_orders(market_id).await;
            return;
        }

        let up_base = clamp_price(max_total_cost - down_ask);
        let down_base = clamp_price(max_total_cost - up_ask);

        let levels = self.config.legging_config.max_live_orders_per_token.max(1);
        let size_per_order = self.config.arb_config.shares_per_side;

        let mut new_up_orders = Vec::new();
        let mut new_down_orders = Vec::new();

        let mut remaining_up_size = remaining_up;
        if remaining_up_size > Decimal::ZERO {
            for i in 0..levels {
                if remaining_up_size <= Decimal::ZERO {
                    break;
                }
                let price = clamp_price(up_base - LADDER_PRICE_STEP * Decimal::from(i as u32));
                let down_ref = inv.down_avg.unwrap_or(down_ask);
                if price + down_ref > max_total_cost {
                    continue;
                }
                let size = size_per_order.min(remaining_up_size);
                new_up_orders.push((price, size));
                remaining_up_size -= size;
            }
        }

        let mut remaining_down_size = remaining_down;
        if remaining_down_size > Decimal::ZERO {
            for i in 0..levels {
                if remaining_down_size <= Decimal::ZERO {
                    break;
                }
                let price = clamp_price(down_base - LADDER_PRICE_STEP * Decimal::from(i as u32));
                let up_ref = inv.up_avg.unwrap_or(up_ask);
                if price + up_ref > max_total_cost {
                    continue;
                }
                let size = size_per_order.min(remaining_down_size);
                new_down_orders.push((price, size));
                remaining_down_size -= size;
            }
        }

        self.replace_side_orders(market_id, Side::Up, &new_up_orders).await;
        self.replace_side_orders(market_id, Side::Down, &new_down_orders).await;
    }

    async fn replace_side_orders(
        &mut self,
        market_id: &str,
        side: Side,
        desired: &[(Decimal, Decimal)],
    ) {
        let mut existing = {
            let orders = self.orders.entry(market_id.to_string()).or_default();
            match side {
                Side::Up => std::mem::take(&mut orders.up),
                Side::Down => std::mem::take(&mut orders.down),
            }
        };

        if !existing.is_empty() {
            let ids: Vec<String> = existing.iter().map(|o| o.order_id.clone()).collect();
            self.cancel_orders(&ids).await;
            existing.clear();
        }

        for (price, size) in desired {
            if *size <= Decimal::ZERO {
                continue;
            }
            if let Some(order_id) = self.place_ladder_order(market_id, side, *price, *size).await {
                existing.push(RestingOrder {
                    order_id,
                    price: *price,
                    size: *size,
                });
            }
        }

        let orders = self.orders.entry(market_id.to_string()).or_default();
        match side {
            Side::Up => orders.up = existing,
            Side::Down => orders.down = existing,
        }
    }

    async fn cancel_all_orders(&mut self, market_id: &str) {
        let ids: Vec<String> = self
            .orders
            .get(market_id)
            .map(|orders| {
                orders
                    .up
                    .iter()
                    .chain(orders.down.iter())
                    .map(|o| o.order_id.clone())
                    .collect()
            })
            .unwrap_or_default();

        if !ids.is_empty() {
            self.cancel_orders(&ids).await;
        }

        if let Some(orders) = self.orders.get_mut(market_id) {
            orders.up.clear();
            orders.down.clear();
        }
    }

    async fn cancel_orders(&self, order_ids: &[String]) {
        if self.config.dry_run {
            return;
        }
        let refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();
        let _ = self.client.cancel_orders(&refs).await;
    }

    async fn place_ladder_order(
        &self,
        market_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
    ) -> Option<String> {
        let state = self.markets.get(market_id)?;
        let token_id = match side {
            Side::Up => state.info.up_token_id.as_str(),
            Side::Down => state.info.down_token_id.as_str(),
        };

        let poly_price = PolyDecimal::try_from(price.to_string().as_str()).ok()?;
        let poly_size = PolyDecimal::try_from(size.to_string().as_str()).ok()?;

        if self.config.dry_run {
            return Some(format!("dry-{}-{}", token_id, Utc::now().timestamp_millis()));
        }

        let signable = self
            .client
            .limit_order()
            .token_id(token_id)
            .price(poly_price)
            .size(poly_size)
            .side(ClobSide::Buy)
            .order_type(OrderType::GTC)
            .build()
            .await
            .ok()?;

        let signed = self
            .client
            .sign(&self.signer.clone().with_chain_id(Some(POLYGON)), signable)
            .await
            .ok()?;

        let response = self.client.post_order(signed).await.ok()?;
        Some(response.order_id)
    }

    fn log_status(&self) {
        let now = Utc::now();
        let est_time = now.with_timezone(&New_York).format("%H:%M:%S");

        info!(
            time_est = %est_time,
            markets = self.markets.len(),
            "Status update"
        );

        for (market_id, state) in &self.markets {
            let pair = state.pair.read();
            let inv = self.inventory.get(market_id).cloned().unwrap_or_default();
            let combined = match (inv.up_avg, inv.down_avg) {
                (Some(u), Some(d)) => Some(u + d),
                _ => None,
            };
            let edge = combined.map(|c| dec!(1.0) - c);

            info!(
                asset = %state.info.asset.to_uppercase(),
                elapsed_pct = format!("{:.1}%", state.elapsed_pct(now) * 100.0),
                up_ask = pair.rest_up_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                down_ask = pair.rest_down_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                up_shares = %inv.up_shares.round_dp(2),
                down_shares = %inv.down_shares.round_dp(2),
                combined = combined.map(|c| format!("{:.3}", c)).unwrap_or_else(|| "---".to_string()),
                edge = edge.map(|e| format!("{:.3}", e)).unwrap_or_else(|| "---".to_string()),
                "Market status"
            );
        }
    }

    #[instrument(skip(self))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");

        let all_markets = match self.market_cache.get_markets(Utc::now()).await {
            Ok(markets) => markets,
            Err(e) => {
                error!(error = %e, "Failed to fetch markets from cache");
                return;
            }
        };

        let now = Utc::now();
        let active_markets: Vec<_> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

        info!(
            total_active = active_markets.len(),
            "Filtering to one market per asset"
        );

        // Select one market per asset (first encountered)
        let mut selected: HashMap<String, _> = HashMap::new();
        for market in active_markets {
            selected.entry(market.asset.clone()).or_insert(market);
        }

        info!(selected_count = selected.len(), "Markets selected");

        self.markets.clear();
        self.trading_pairs.clear();
        self.token_to_market.clear();
        self.token_to_side.clear();

        for (asset, info) in selected {
            let market_id = info.id.clone();
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            let open_time = Utc::now();

            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol: String::new(),
                start_time: open_time,
                end_time: open_time + DEFAULT_WINDOW_DURATION,
                binance_open_price: None,
            };

            info!(
                market_id = %market_id,
                asset = %asset,
                end_time = %state.end_time,
                "Market registered"
            );

            self.markets.insert(market_id.clone(), state);
            self.trading_pairs.insert(market_id, pair);
            self.token_to_market
                .insert(info.up_token_id.clone(), info.id.clone());
            self.token_to_market
                .insert(info.down_token_id.clone(), info.id.clone());
            self.token_to_side
                .insert(info.up_token_id.clone(), Side::Up);
            self.token_to_side
                .insert(info.down_token_id.clone(), Side::Down);
        }

        self.current_window_end = self.markets.values().map(|s| s.end_time).min();

        info!(
            market_count = self.markets.len(),
            window_end = ?self.current_window_end,
            "Market discovery complete"
        );
    }

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    async fn update_prices(&mut self) {
        // Collect market info first to avoid borrow conflicts
        let market_info: Vec<_> = self
            .markets
            .values()
            .map(|state| {
                (
                    state.info.id.clone(),
                    state.info.asset.clone(),
                    state.info.up_token_id.clone(),
                    state.info.down_token_id.clone(),
                )
            })
            .collect();

        // Update Polymarket prices
        let client = reqwest::Client::new();
        for (market_id, _, up_token_id, down_token_id) in &market_info {
            // Up token
            match crate::ws::clob::fetch_token_price(&client, up_token_id).await {
                Ok(Some((bid, ask))) => {
                    if let Some(pair) = self.trading_pairs.get(market_id) {
                        let mut p = pair.write();
                        p.rest_up_bid = Some(bid);
                        p.rest_up_ask = Some(ask);
                        p.last_rest_update_ms = crate::models::now_ms();
                    }
                }
                Ok(None) => {
                    debug!(
                        market_id,
                        token_id = %up_token_id,
                        side = "up",
                        "No price data returned"
                    );
                }
                Err(e) => {
                    debug!(
                        market_id,
                        token_id = %up_token_id,
                        side = "up",
                        error = %e,
                        "Failed to fetch token price"
                    );
                }
            }

            // Down token
            match crate::ws::clob::fetch_token_price(&client, down_token_id).await {
                Ok(Some((bid, ask))) => {
                    if let Some(pair) = self.trading_pairs.get(market_id) {
                        let mut p = pair.write();
                        p.rest_down_bid = Some(bid);
                        p.rest_down_ask = Some(ask);
                        p.last_rest_update_ms = crate::models::now_ms();
                    }
                }
                Ok(None) => {
                    debug!(
                        market_id,
                        token_id = %down_token_id,
                        side = "down",
                        "No price data returned"
                    );
                }
                Err(e) => {
                    debug!(
                        market_id,
                        token_id = %down_token_id,
                        side = "down",
                        error = %e,
                        "Failed to fetch token price"
                    );
                }
            }
        }
    }

    fn should_rotate_markets(&self, now: DateTime<Utc>) -> bool {
        self.current_window_end.map_or(true, |end| now >= end)
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}
