use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{MarketState, Side as OutcomeSide, TradingPair};

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::request::TradesRequest;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::types::Side as ClobTradeSide;
use polymarket_client_sdk::clob::types::request::{CancelMarketOrderRequest, OrdersRequest};
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::types::Address;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::ToQueryParams as _;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};
use futures_util::stream::{FuturesUnordered, StreamExt};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const STATUS_PRINT_INTERVAL: Duration = Duration::from_secs(5);
const PRICE_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const SCAN_LOOP_DELAY: Duration = Duration::from_millis(100);
const INVENTORY_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const ORDER_GUARD_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

const TICK_SIZE_DP: u32 = 2; // 0.01 tick size
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);

fn poly_to_rust_decimal(d: &PolyDecimal) -> Option<Decimal> {
    d.to_string().parse::<Decimal>().ok()
}

fn round_to_tick_up(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::AwayFromZero)
}

fn calculate_aggressive_buy_limit(observed_ask: Decimal, buffer: Decimal) -> Decimal {
    round_to_tick_up(observed_ask + buffer).max(MIN_PRICE).min(MAX_PRICE)
}

#[derive(Debug, Default, Clone)]
struct MarketInventory {
    up_shares: Decimal,
    down_shares: Decimal,
    up_cost: Decimal,
    down_cost: Decimal,
    seen_trade_ids: HashSet<String>,
}

impl MarketInventory {
    fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }

    fn avg_cost_excess_side(&self) -> Option<Decimal> {
        let imbalance = self.imbalance();
        if imbalance > Decimal::ZERO {
            if self.up_shares > Decimal::ZERO {
                return Some(self.up_cost / self.up_shares);
            }
        } else if imbalance < Decimal::ZERO {
            if self.down_shares > Decimal::ZERO {
                return Some(self.down_cost / self.down_shares);
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
struct MopContext {
    market_id: String,
    asset: String,
    end_time: DateTime<Utc>,
    up_token_id: String,
    down_token_id: String,
    pair: Arc<RwLock<TradingPair>>,
}

#[derive(Debug, Default, Clone)]
struct OpenOrdersSummary {
    total: usize,
    up_token: usize,
    down_token: usize,
}

pub struct LeggingBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    http: reqwest::Client,
    signer: PrivateKeySigner,
    trader_address: Address,
    signature_type: SignatureType,
    market_cache: MarketCache,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<crate::models::TradingPair>>>,
    inventory_by_market: HashMap<String, MarketInventory>,
    last_inventory_refresh: HashMap<String, Instant>,
    last_order_guard_refresh: HashMap<String, Instant>,
    market_has_live_orders: HashMap<String, bool>,
    last_status_print: Instant,
    last_price_update: Instant,
    last_trade_by_market: HashMap<String, Instant>,
}

impl LeggingBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing LeggingBot");

        let market_cache = MarketCache::new(config.target_assets.clone());

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;
        info!(
            signer_address = %signer.address(),
            proxy_address = %config.polymarket_proxy_address,
            "Loaded signer"
        );

        info!("Authenticating with Polymarket CLOB");
        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));
        let mut auth = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer_with_chain);

        // If you trade via a Polymarket proxy wallet, you must set both `funder` and `signature_type`.
        let (signature_type, funder, trader_address) = if config.polymarket_proxy_address.trim().is_empty() {
            let addr = Address::from_str(&signer.address().to_string())
                .map_err(|e| anyhow::anyhow!("Failed to parse signer address: {}", e))?;
            (SignatureType::Eoa, None, addr)
        } else {
            let addr = Address::from_str(config.polymarket_proxy_address.trim())
                .map_err(|e| anyhow::anyhow!("Invalid POLYMARKET_PROXY_ADDRESS: {}", e))?;
            auth = auth.funder(addr).signature_type(SignatureType::Proxy);
            (SignatureType::Proxy, Some(addr), addr)
        };

        let client = auth.authenticate().await?;

        // Preflight: log the exact balance-allowance URL (with encoded params) and the response.
        let bal_req = BalanceAllowanceRequest::builder()
            .asset_type(polymarket_client_sdk::clob::types::AssetType::Collateral)
            .signature_type(signature_type)
            .build();
        let bal_url = format!(
            "{}/balance-allowance{}",
            crate::config::POLYMARKET_CLOB_HOST,
            bal_req.query_params(None)
        );
        info!(
            url = %bal_url,
            signature_type = ?signature_type,
            funder = ?funder,
            "Checking collateral balance/allowance"
        );
        match client.balance_allowance(bal_req).await {
            Ok(resp) => {
                info!(
                    balance = %resp.balance,
                    allowances = ?resp.allowances,
                    "CLOB balance/allowance"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to fetch balance/allowance");
            }
        }

        Ok(Self {
            config,
            client: Arc::new(client),
            http: reqwest::Client::new(),
            signer,
            trader_address,
            signature_type,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            inventory_by_market: HashMap::new(),
            last_inventory_refresh: HashMap::new(),
            last_order_guard_refresh: HashMap::new(),
            market_has_live_orders: HashMap::new(),
            last_status_print: Instant::now(),
            last_price_update: Instant::now(),
            last_trade_by_market: HashMap::new(),
        })
    }

    pub async fn run(&mut self) {
        info!(
            dry_run = self.config.dry_run,
            shares_per_side = %self.config.arb_config.shares_per_side,
            max_total_cost = %self.config.legging_config.max_total_cost,
            mop_start_secs = self.config.legging_config.mop_start_secs,
            mop_max_total_cost = %self.config.legging_config.mop_max_total_cost,
            end_target_imbalance = %self.config.legging_config.end_target_imbalance_shares,
            taker_buffer = %self.config.legging_config.taker_buffer,
            cooldown_secs = self.config.legging_config.cooldown_secs,
            "Bot starting - legging strategy"
        );
        if self.config.legging_config.max_total_cost >= dec!(1.00) {
            warn!(
                max_total_cost = %self.config.legging_config.max_total_cost,
                "max_total_cost at/above 1.00 leaves little/no edge after fees and rounding"
            );
        }

        self.discover_markets().await;
        info!("Entering main loop");

        loop {
            if self.last_price_update.elapsed() >= PRICE_UPDATE_INTERVAL {
                self.update_pm_prices().await;
                self.last_price_update = Instant::now();
            }

            if self.last_status_print.elapsed() >= STATUS_PRINT_INTERVAL {
                self.log_status();
                self.last_status_print = Instant::now();
            }

            self.scan().await;

            tokio::time::sleep(SCAN_LOOP_DELAY).await;
        }
    }

    #[instrument(skip(self))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");

        let all_markets = match self.market_cache.get_markets(Utc::now()).await {
            Ok(markets) => markets,
            Err(e) => {
                error!(error = %e, "Failed to fetch markets from Gamma API");
                return;
            }
        };

        let now = Utc::now();
        let active_markets: Vec<_> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

        // Select one market per asset (first encountered).
        let mut selected: HashMap<String, _> = HashMap::new();
        for market in active_markets {
            selected.entry(market.asset.clone()).or_insert(market);
        }

        self.markets.clear();
        self.trading_pairs.clear();

        for (asset, info) in selected {
            let market_id = info.id.clone();
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));

            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol: String::new(),
                start_time: info.start_time,
                end_time: info.end_time,
                binance_open_price: None,
            };

            info!(
                market_id = %market_id,
                asset = %asset,
                up = %state.info.up_token_id,
                down = %state.info.down_token_id,
                "Market registered"
            );

            self.markets.insert(market_id.clone(), state);
            self.trading_pairs.insert(market_id, pair);
        }

        info!(market_count = self.markets.len(), "Market discovery complete");
    }

    async fn update_pm_prices(&mut self) {
        let market_info: Vec<_> = self
            .markets
            .values()
            .map(|state| {
                (
                    state.info.id.clone(),
                    state.info.up_token_id.clone(),
                    state.info.down_token_id.clone(),
                )
            })
            .collect();

        // Fetch per-market token prices concurrently to reduce latency.
        let http = &self.http;
        let mut futs = FuturesUnordered::new();
        for (market_id, up_token_id, down_token_id) in market_info {
            let market_id2 = market_id.clone();
            let up_token_id2 = up_token_id.clone();
            let down_token_id2 = down_token_id.clone();
            futs.push(async move {
                let (up, down) = tokio::join!(
                    crate::ws::clob::fetch_token_price(http, &up_token_id2),
                    crate::ws::clob::fetch_token_price(http, &down_token_id2)
                );
                (market_id2, up, down)
            });
        }

        while let Some((market_id, up, down)) = futs.next().await {
            if let Some(pair) = self.trading_pairs.get(&market_id) {
                let mut p = pair.write();
                let now_ms = crate::models::now_ms();
                if let Ok(Some((bid, ask))) = up {
                    p.rest_up_bid = Some(bid);
                    p.rest_up_ask = Some(ask);
                    p.last_rest_update_ms = now_ms;
                }
                if let Ok(Some((bid, ask))) = down {
                    p.rest_down_bid = Some(bid);
                    p.rest_down_ask = Some(ask);
                    p.last_rest_update_ms = now_ms;
                }
            }
        }
    }

    async fn scan(&mut self) {
        let now = Instant::now();
        let now_ms = crate::models::now_ms();
        let cooldown = Duration::from_secs(self.config.legging_config.cooldown_secs);

        let market_ids: Vec<String> = self.markets.keys().cloned().collect();
        for market_id in market_ids {
            let mop_ctx = {
                let Some(state) = self.markets.get(&market_id) else { continue };
                let remaining_secs = (state.info.end_time - Utc::now()).num_seconds();
                if remaining_secs <= self.config.legging_config.mop_start_secs {
                    Some(MopContext {
                        market_id: market_id.clone(),
                        asset: state.info.asset.clone(),
                        end_time: state.info.end_time,
                        up_token_id: state.info.up_token_id.clone(),
                        down_token_id: state.info.down_token_id.clone(),
                        pair: state.pair.clone(),
                    })
                } else {
                    None
                }
            };
            if let Some(ctx) = mop_ctx {
                self.mop_toward_flat(ctx, now).await;
                continue;
            }

            let (asset, up_token_id, down_token_id, pair_arc) = {
                let Some(state) = self.markets.get(&market_id) else { continue };
                (
                    state.info.asset.clone(),
                    state.info.up_token_id.clone(),
                    state.info.down_token_id.clone(),
                    state.pair.clone(),
                )
            };

            let pair = pair_arc.read();

            if pair.last_rest_update_ms > 0
                && (now_ms - pair.last_rest_update_ms) > self.config.legging_config.max_price_age_ms
            {
                debug!(
                    market_id,
                    age_ms = now_ms - pair.last_rest_update_ms,
                    "Skipping: stale prices"
                );
                continue;
            }

            let (up_ask, down_ask) = match (pair.latest_up_ask(), pair.latest_down_ask()) {
                (Some(u), Some(d)) => (u, d),
                _ => continue,
            };

            if let Some(last) = self.last_trade_by_market.get(&market_id) {
                if last.elapsed() < cooldown {
                    continue;
                }
            }

            let up_limit = calculate_aggressive_buy_limit(up_ask, self.config.legging_config.taker_buffer);
            let down_limit =
                calculate_aggressive_buy_limit(down_ask, self.config.legging_config.taker_buffer);
            let total_cost = up_limit + down_limit;

            if total_cost > self.config.legging_config.max_total_cost {
                continue;
            }

            info!(
                market_id,
                asset = %asset,
                up_ask = %up_ask,
                down_ask = %down_ask,
                up_limit = %up_limit,
                down_limit = %down_limit,
                total_cost = %total_cost,
                "Legging opportunity detected"
            );

            if self.config.legging_config.prevent_order_pileup {
                match self.market_has_live_orders(&market_id, now).await {
                    Ok(true) => {
                        if self.config.legging_config.manage_pileup_orders {
                            self.maybe_cancel_pileup_and_equalize(
                                &market_id,
                                &asset,
                                &up_token_id,
                                &down_token_id,
                                pair_arc.clone(),
                                now,
                            )
                            .await;
                        }
                        debug!(market_id, "Skipping: market has live orders");
                        continue;
                    }
                    Ok(false) => {}
                    Err(e) => {
                        debug!(market_id, error = %e, "Order guard check failed; skipping to avoid piling up");
                        continue;
                    }
                }
            }

            // "Legging": execute two buys back-to-back. This can leave exposure if the second order fails.
            let first = if up_limit <= down_limit {
                (OutcomeSide::Up, &up_token_id, up_limit)
            } else {
                (OutcomeSide::Down, &down_token_id, down_limit)
            };
            let second = if first.0 == OutcomeSide::Up {
                (OutcomeSide::Down, &down_token_id, down_limit)
            } else {
                (OutcomeSide::Up, &up_token_id, up_limit)
            };

            let (first_order_id, second_order_id) = self
                .place_two_leg_orders(
                    first.1,
                    first.2,
                    second.1,
                    second.2,
                    self.config.arb_config.shares_per_side,
                    &market_id,
                )
                .await;

            match (first_order_id, second_order_id) {
                (Some(oid1), Some(oid2)) => {
                    info!(
                        market_id,
                        first_side = ?first.0,
                        first_order_id = %oid1,
                        second_side = ?second.0,
                        second_order_id = %oid2,
                        "Legging executed"
                    );
                    self.last_trade_by_market.insert(market_id.clone(), now);
                }
                (oid1, oid2) => {
                    warn!(
                        market_id,
                        first_side = ?first.0,
                        first_order_id = ?oid1,
                        second_side = ?second.0,
                        second_order_id = ?oid2,
                        "Legging incomplete (exposure risk)"
                    );
                    // Still apply cooldown to avoid rapidly stacking exposure on retries.
                    self.last_trade_by_market.insert(market_id.clone(), now);
                }
            }
        }
    }

    async fn place_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
        market_id: &str,
        label: &str,
    ) -> Option<String> {
        let order_url = format!("{}/order", crate::config::POLYMARKET_CLOB_HOST);
        info!(
            market_id,
            token_id = %token_id,
            price = %price,
            size = %size,
            dry_run = self.config.dry_run,
            label,
            url = %order_url,
            "Placing order"
        );

        if self.config.dry_run {
            return Some(format!("dry-run-{}-{}", label, Utc::now().timestamp_millis()));
        }

        let poly_price = match PolyDecimal::try_from(price.to_string().as_str()) {
            Ok(p) => p,
            Err(e) => {
                error!(market_id, error = %e, "Invalid price format");
                return None;
            }
        };

        let poly_size = match PolyDecimal::try_from(size.to_string().as_str()) {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Invalid size format");
                return None;
            }
        };

        let signable = match self
            .client
            .limit_order()
            .token_id(token_id)
            .price(poly_price)
            .size(poly_size)
            .side(ClobSide::Buy)
            .build()
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Failed to build order");
                return None;
            }
        };

        let signed = match self
            .client
            .sign(
                &self.signer.clone().with_chain_id(Some(POLYGON)),
                signable,
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Failed to sign order");
                return None;
            }
        };

        match self.client.post_order(signed).await {
            Ok(response) => {
                if let Some(ref error_msg) = response.error_msg {
                    if !error_msg.is_empty() {
                        warn!(
                            market_id,
                            token_id = %token_id,
                            error_msg,
                            order_id = %response.order_id,
                            "Order posted with error message"
                        );
                    }
                }
                Some(response.order_id)
            }
            Err(e) => {
                error!(market_id, error = %e, "Order post failed");
                None
            }
        }
    }

    fn log_status(&self) {
        info!(
            markets = self.markets.len(),
            cooldown_secs = self.config.legging_config.cooldown_secs,
            "Status update"
        );
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    async fn mop_toward_flat(&mut self, ctx: MopContext, now: Instant) {
        let pair = ctx.pair.read();
        let (up_ask, down_ask) = match (pair.latest_up_ask(), pair.latest_down_ask()) {
            (Some(u), Some(d)) => (u, d),
            _ => return,
        };

        let should_refresh = self
            .last_inventory_refresh
            .get(&ctx.market_id)
            .map(|t| t.elapsed() >= INVENTORY_REFRESH_INTERVAL)
            .unwrap_or(true);
        if should_refresh {
            if let Err(e) = self
                .refresh_inventory(&ctx.market_id, &ctx.up_token_id, &ctx.down_token_id)
                .await
            {
                debug!(market_id = %ctx.market_id, error = %e, "Inventory refresh failed");
            }
            self.last_inventory_refresh.insert(ctx.market_id.clone(), now);
        }

        let inv = match self.inventory_by_market.get(&ctx.market_id) {
            Some(i) => i,
            None => return,
        };

        let target = self.config.legging_config.end_target_imbalance_shares;
        let imbalance = inv.imbalance();
        let abs_imbalance = imbalance.abs();
        if abs_imbalance <= target {
            return;
        }

        // Buy the missing side to reduce imbalance, but only if it pairs with existing excess
        // inventory at a combined cost under the mop threshold.
        let (buy_side, token_id, observed_ask, other_ask) = if imbalance > Decimal::ZERO {
            (OutcomeSide::Down, &ctx.down_token_id, down_ask, up_ask)
        } else {
            (OutcomeSide::Up, &ctx.up_token_id, up_ask, down_ask)
        };

        let limit_price =
            calculate_aggressive_buy_limit(observed_ask, self.config.legging_config.taker_buffer);

        let excess_avg_cost = inv.avg_cost_excess_side().unwrap_or(other_ask);
        let combined_cost = excess_avg_cost + limit_price;
        if combined_cost > self.config.legging_config.mop_max_total_cost {
            debug!(
                market_id = %ctx.market_id,
                buy_side = ?buy_side,
                limit_price = %limit_price,
                excess_avg_cost = %excess_avg_cost,
                combined_cost = %combined_cost,
                mop_max = %self.config.legging_config.mop_max_total_cost,
                "Mop skip: combined cost too high"
            );
            return;
        }

        let needed = abs_imbalance - target;
        let order_size = needed.min(self.config.arb_config.shares_per_side);

        info!(
            market_id = %ctx.market_id,
            asset = %ctx.asset,
            remaining_secs = (ctx.end_time - Utc::now()).num_seconds(),
            buy_side = ?buy_side,
            order_size = %order_size,
            limit_price = %limit_price,
            combined_cost = %combined_cost,
            "Endgame mop: placing flattening order"
        );

        let _ = self
            .place_order(token_id, limit_price, order_size, &ctx.market_id, "mop")
            .await;
        self.last_trade_by_market
            .insert(ctx.market_id, now);
    }

    async fn market_has_live_orders(&mut self, market_id: &str, now: Instant) -> anyhow::Result<bool> {
        let should_refresh = self
            .last_order_guard_refresh
            .get(market_id)
            .map(|t| t.elapsed() >= ORDER_GUARD_REFRESH_INTERVAL)
            .unwrap_or(true);

        if !should_refresh {
            return Ok(*self.market_has_live_orders.get(market_id).unwrap_or(&false));
        }

        let req = OrdersRequest::builder().market(market_id).build();
        let page = self.client.orders(&req, None).await?;
        let has_live = !page.data.is_empty();
        self.last_order_guard_refresh
            .insert(market_id.to_string(), now);
        self.market_has_live_orders
            .insert(market_id.to_string(), has_live);
        Ok(has_live)
    }

    async fn open_orders_summary(&self, market_id: &str, up_token_id: &str, down_token_id: &str) -> anyhow::Result<OpenOrdersSummary> {
        let req = OrdersRequest::builder().market(market_id).build();
        let mut cursor: Option<String> = None;
        let mut summary = OpenOrdersSummary::default();

        loop {
            let page = self.client.orders(&req, cursor.clone()).await?;
            for o in page.data {
                summary.total += 1;
                if o.asset_id == up_token_id {
                    summary.up_token += 1;
                } else if o.asset_id == down_token_id {
                    summary.down_token += 1;
                }
                if summary.total >= (self.config.legging_config.max_live_orders_per_market + 1) {
                    return Ok(summary);
                }
            }

            if page.next_cursor == "LTE=" || page.count == 0 {
                break;
            }
            cursor = Some(page.next_cursor);
        }

        Ok(summary)
    }

    async fn place_two_leg_orders(
        &self,
        token_id_a: &str,
        price_a: Decimal,
        token_id_b: &str,
        price_b: Decimal,
        size: Decimal,
        market_id: &str,
    ) -> (Option<String>, Option<String>) {
        // Fast path for dry-run.
        if self.config.dry_run {
            let id1 = self.place_order(token_id_a, price_a, size, market_id, "first_leg").await;
            let id2 = self.place_order(token_id_b, price_b, size, market_id, "second_leg").await;
            return (id1, id2);
        }

        // Build+sign both, then submit a single /orders request to reduce latency.
        let price_a_poly = match PolyDecimal::try_from(price_a.to_string().as_str()) {
            Ok(p) => p,
            Err(e) => {
                error!(market_id, error = %e, "Invalid price_a format");
                return (None, None);
            }
        };
        let price_b_poly = match PolyDecimal::try_from(price_b.to_string().as_str()) {
            Ok(p) => p,
            Err(e) => {
                error!(market_id, error = %e, "Invalid price_b format");
                return (None, None);
            }
        };
        let size_poly = match PolyDecimal::try_from(size.to_string().as_str()) {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Invalid size format");
                return (None, None);
            }
        };

        let a_signable = match self
            .client
            .limit_order()
            .token_id(token_id_a)
            .price(price_a_poly)
            .size(size_poly.clone())
            .side(ClobSide::Buy)
            .build()
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Failed to build first leg order");
                return (None, None);
            }
        };
        let b_signable = match self
            .client
            .limit_order()
            .token_id(token_id_b)
            .price(price_b_poly)
            .size(size_poly)
            .side(ClobSide::Buy)
            .build()
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Failed to build second leg order");
                return (None, None);
            }
        };

        let signer_with_chain = self.signer.clone().with_chain_id(Some(POLYGON));
        let a_signed = match self
            .client
            .sign(&signer_with_chain, a_signable)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Failed to sign first leg order");
                return (None, None);
            }
        };
        let b_signed = match self
            .client
            .sign(&signer_with_chain, b_signable)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(market_id, error = %e, "Failed to sign second leg order");
                return (None, None);
            }
        };

        let url = format!("{}/orders", crate::config::POLYMARKET_CLOB_HOST);
        info!(
            market_id,
            token_a = %token_id_a,
            price_a = %price_a,
            token_b = %token_id_b,
            price_b = %price_b,
            size = %size,
            url = %url,
            "Posting two-leg batch"
        );

        match self.client.post_orders(vec![a_signed, b_signed]).await {
            Ok(responses) => {
                let mut ids = responses.into_iter().map(|r| r.order_id);
                (ids.next(), ids.next())
            }
            Err(e) => {
                error!(market_id, error = %e, "Batch post failed");
                (None, None)
            }
        }
    }

    async fn maybe_cancel_pileup_and_equalize(
        &mut self,
        market_id: &str,
        asset: &str,
        up_token_id: &str,
        down_token_id: &str,
        pair: Arc<RwLock<TradingPair>>,
        now: Instant,
    ) {
        let summary = match self.open_orders_summary(market_id, up_token_id, down_token_id).await {
            Ok(s) => s,
            Err(e) => {
                debug!(market_id, error = %e, "Failed to summarize open orders");
                return;
            }
        };

        let max_market = self.config.legging_config.max_live_orders_per_market;
        let max_token = self.config.legging_config.max_live_orders_per_token;

        let pileup = summary.total > max_market
            || summary.up_token > max_token
            || summary.down_token > max_token;

        if !pileup {
            return;
        }

        warn!(
            market_id,
            asset,
            open_orders_total = summary.total,
            open_orders_up = summary.up_token,
            open_orders_down = summary.down_token,
            max_market,
            max_token,
            "Open order pile-up detected; canceling market orders"
        );

        if !self.config.dry_run {
            let req = CancelMarketOrderRequest::builder().market(market_id).build();
            match self.client.cancel_market_orders(&req).await {
                Ok(resp) => {
                    info!(
                        market_id,
                        canceled = resp.canceled.len(),
                        not_canceled = resp.not_canceled.len(),
                        "Canceled piled-up orders"
                    );
                }
                Err(e) => {
                    warn!(market_id, error = %e, "Failed to cancel market orders");
                    return;
                }
            }
        }

        self.market_has_live_orders.insert(market_id.to_string(), false);
        self.last_order_guard_refresh.insert(market_id.to_string(), now);

        // If we have an inventory imbalance and can flatten at a net-positive edge, do so.
        if let Err(e) = self.refresh_inventory(market_id, up_token_id, down_token_id).await {
            debug!(market_id, error = %e, "Inventory refresh failed after cancel");
            return;
        }
        let Some(inv) = self.inventory_by_market.get(market_id) else { return };

        let imbalance = inv.imbalance();
        let abs_imbalance = imbalance.abs();
        let target = self.config.legging_config.end_target_imbalance_shares;
        if abs_imbalance <= target {
            return;
        }

        let pair = pair.read();
        let (up_ask, down_ask) = match (pair.latest_up_ask(), pair.latest_down_ask()) {
            (Some(u), Some(d)) => (u, d),
            _ => return,
        };

        let (missing_token_id, missing_ask, excess_ask_fallback) = if imbalance > Decimal::ZERO {
            (down_token_id, down_ask, up_ask)
        } else {
            (up_token_id, up_ask, down_ask)
        };

        let limit_price =
            calculate_aggressive_buy_limit(missing_ask, self.config.legging_config.taker_buffer);
        let excess_avg_cost = inv.avg_cost_excess_side().unwrap_or(excess_ask_fallback);
        let combined_cost = excess_avg_cost + limit_price;

        if combined_cost >= self.config.legging_config.equalize_max_total_cost {
            debug!(
                market_id,
                combined_cost = %combined_cost,
                max = %self.config.legging_config.equalize_max_total_cost,
                "Equalize skip: not net-positive"
            );
            return;
        }

        let needed = abs_imbalance - target;
        let order_size = needed.min(self.config.arb_config.shares_per_side);
        info!(
            market_id,
            asset,
            order_size = %order_size,
            limit_price = %limit_price,
            combined_cost = %combined_cost,
            "Equalizing after cancel at net-positive edge"
        );

        let _ = self
            .place_order(missing_token_id, limit_price, order_size, market_id, "equalize")
            .await;
    }

    async fn refresh_inventory(
        &mut self,
        market_id: &str,
        up_token_id: &str,
        down_token_id: &str,
    ) -> anyhow::Result<()> {
        let inv = self
            .inventory_by_market
            .entry(market_id.to_string())
            .or_insert_with(MarketInventory::default);

        let mut trades: Vec<polymarket_client_sdk::clob::types::response::TradeResponse> = Vec::new();

        for (label, req) in [
            (
                "maker",
                TradesRequest::builder()
                    .market(market_id)
                    .maker_address(self.trader_address)
                    .build(),
            ),
            (
                "taker",
                TradesRequest::builder()
                    .market(market_id)
                    .taker_address(self.trader_address)
                    .build(),
            ),
        ] {
            let mut cursor: Option<String> = None;
            loop {
                let page = match self.client.trades(&req, cursor.clone()).await {
                    Ok(p) => p,
                    Err(e) => {
                        debug!(market_id, label, error = %e, "Trades query failed");
                        break;
                    }
                };
                trades.extend(page.data);
                if page.next_cursor == "LTE=" || page.count == 0 {
                    break;
                }
                cursor = Some(page.next_cursor);
            }
        }

        // De-dupe and apply only unseen trades.
        for t in trades {
            if !inv.seen_trade_ids.insert(t.id.clone()) {
                continue;
            }

            let size = match poly_to_rust_decimal(&t.size) {
                Some(v) => v,
                None => continue,
            };
            let price = match poly_to_rust_decimal(&t.price) {
                Some(v) => v,
                None => continue,
            };

            let (side_shares, side_cost) = if t.asset_id == up_token_id {
                (&mut inv.up_shares, &mut inv.up_cost)
            } else if t.asset_id == down_token_id {
                (&mut inv.down_shares, &mut inv.down_cost)
            } else {
                continue;
            };

            match t.side {
                ClobTradeSide::Buy => {
                    *side_shares += size;
                    *side_cost += price * size;
                }
                ClobTradeSide::Sell => {
                    *side_shares -= size;
                    *side_cost -= price * size;
                }
                _ => {}
            }
        }

        debug!(
            market_id,
            signature_type = ?self.signature_type,
            trader_address = %self.trader_address,
            up_shares = %inv.up_shares,
            down_shares = %inv.down_shares,
            imbalance = %inv.imbalance(),
            "Inventory refreshed"
        );

        Ok(())
    }
}
