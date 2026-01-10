use crate::config::Config;
use crate::market_cache::MarketCache;
use crate::models::{MarketState, TradingPair};
use crate::poller::{InventoryLedger, spawn_order_poller, MarketSide};
use crate::clob_rest::{spawn_price_feed, PriceCache};

use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{Side as ClobSide, SignatureType};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal as PolyDecimal};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(200);
const MARKET_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const STATUS_PRINT_INTERVAL: Duration = Duration::from_secs(5);

const TICK_SIZE_DP: u32 = 2;
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const MIN_ORDER_NOTIONAL: Decimal = dec!(1.00);
const WS_SUB_URL: &str = "wss://ws-subscriptions-clob.polymarket.com";


pub struct SimpleBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    last_market_refresh: Instant,
    last_order_by_market: HashMap<String, Instant>,
    last_status_print: Instant,

    ledger: Arc<RwLock<InventoryLedger>>,
    price_cache: Arc<RwLock<PriceCache>>,
    _order_poller: tokio::task::JoinHandle<()>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let price_cache = Arc::new(RwLock::new(PriceCache::default()));

        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)
            .map_err(|e| anyhow::anyhow!("Failed to parse private key: {}", e))?;

        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));
        
        let addr = Address::from_str(config.polymarket_proxy_address.trim())
            .map_err(|e| anyhow::anyhow!("Invalid POLYMARKET_PROXY_ADDRESS: {}", e))?;
        
        let client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer_with_chain)
            .funder(addr)
            .signature_type(SignatureType::Proxy)
            .authenticate()
            .await?;

        let client = Arc::new(client);

        let ledger = Arc::new(RwLock::new(InventoryLedger::default()));
        let poller = spawn_order_poller(client.clone(), ledger.clone(), Duration::from_millis(500));

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            last_status_print: Instant::now(),
            ledger,
            _order_poller: poller,
            price_cache,
            _price_feed: None,
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;
        info!("Entering main loop...");

        loop {
            if self.last_market_refresh.elapsed() > MARKET_REFRESH_INTERVAL {
                self.discover_markets().await;
                self.last_market_refresh = Instant::now();
            }

            if self.last_status_print.elapsed() > STATUS_PRINT_INTERVAL {
                self.log_status();
                self.last_status_print = Instant::now();
            }

            self.scan().await;

            tokio::time::sleep(LOOP_DELAY).await;
        }
    }

    #[instrument(skip(self), fields(markets = self.markets.len()))]
    pub async fn discover_markets(&mut self) {
        info!("Discovering markets");
        let all_markets = match self.market_cache.get_markets(Utc::now()).await {
            Ok(markets) => markets,
            Err(e) => {
                error!(error = %e, "Failed to fetch markets");
                return;
            }
        };

        let now = Utc::now();
        let active_markets: Vec<_> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

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
                start_time: info.start_time,
                end_time: info.end_time,
            };

            info!(
                market_id = %market_id,
                asset = %asset,
                end_time = %state.end_time,
                "Market registered"
            );

            self.markets.insert(market_id.clone(), state);
            self.trading_pairs.insert(market_id, pair);
        }

        let token_ids: Vec<String> = self.markets.values().flat_map(|m| vec![
            m.info.up_token_id.clone(),
            m.info.down_token_id.clone()
        ]).collect();
        
        if let Some(handle) = self._price_feed.take() {
            handle.abort();
        }

        match spawn_price_feed(WS_SUB_URL, token_ids, self.price_cache.clone()) {
            Ok(handle) => {
                self._price_feed = Some(handle);
                info!("Price feed started!!");
            }
            Err(e) => {
                error!(error = %e, "Failed to start price feed :(");
            }
        }
    }

    async fn scan(&mut self) {
        let market_ids: Vec<String> = self.markets.keys().cloned().collect();

        for market_id in market_ids {

            let (up_token_id, down_token_id) = match self.markets.get(&market_id) {
                Some(s) => (s.info.up_token_id.clone(), s.info.down_token_id.clone()),
                None => continue,
            };

            let price_cache = self.price_cache.read();

            let (up_ask, down_ask) = match (
                price_cache.get(&up_token_id),
                price_cache.get(&down_token_id),
             ) {
                (Some((_, up_ask)), Some((_, down_ask))) => (up_ask, down_ask),
                _ => {
                    debug!(market_id, "Missing price data");
                    continue;
                }
             };
             drop(price_cache);

            // Get inventory from the ledger (non-blocking read)
            let pos = self.ledger.read().effective_position(&market_id);
            let target = self.config.shares_target_per_side;
            let missing_up = (target - pos.up_shares - pos.pending_up).max(Decimal::ZERO);
            let missing_down = (target - pos.down_shares - pos.pending_down).max(Decimal::ZERO);

            if missing_up.is_zero() && missing_down.is_zero() {
                continue;
            }

            if !self.cooldown_elapsed(&market_id) {
                continue;
            }

            // Place orders based on what's missing
            if missing_up > Decimal::ZERO && missing_down > Decimal::ZERO {
                let size = self.config.order_size.min(missing_up).min(missing_down);
                let (up_price, down_price) = split_target_prices(
                    up_ask,
                    down_ask,
                    self.config.target_total_cost,
                    self.config.maker_price_offset,
                );
                let min_size = min_size_for_notional(up_price).max(min_size_for_notional(down_price));

                if size < min_size {
                    debug!(
                        market_id,
                        size = %size,
                        min_size = %min_size,
                        "Paired order size too small"
                    );
                    continue;
                }

                let up_id = self.place_order(&up_token_id, up_price, size, &market_id, "up").await;
                let down_id = self.place_order(&down_token_id, down_price, size, &market_id, "down").await;

                // Record in ledger for optimistic tracking
                if let Some(ref id) = up_id {
                    self.ledger.write().record_order_placed(
                        id.clone(),
                        market_id.clone(),
                        up_token_id.clone(),
                        MarketSide::Up,
                        size,
                        up_price,
                    );
                }
                if let Some(ref id) = down_id {
                    self.ledger.write().record_order_placed(
                        id.clone(),
                        market_id.clone(),
                        down_token_id.clone(),
                        MarketSide::Down,
                        size,
                        down_price,
                    );
                }

                info!(
                    market_id,
                    up_price = %up_price,
                    down_price = %down_price,
                    size = %size,
                    "Placed paired orders"
                );
                self.last_order_by_market.insert(market_id.clone(), Instant::now());

            } else if missing_up > Decimal::ZERO {
                let size = self.config.order_size.min(missing_up);
                let Some(price) = single_target_price(
                    up_ask,
                    down_ask,
                    self.config.target_total_cost,
                    self.config.maker_price_offset,
                ) else {
                    continue;
                };

                if size < min_size_for_notional(price) {
                    debug!(market_id, size = %size, "Up order too small");
                    continue;
                }

                let order_id = self.place_order(&up_token_id, price, size, &market_id, "up").await;

                if let Some(ref id) = order_id {
                    self.ledger.write().record_order_placed(
                        id.clone(),
                        market_id.clone(),
                        up_token_id.clone(),
                        MarketSide::Up,
                        size,
                        price,
                    );
                }

                info!(market_id, price = %price, size = %size, order = ?order_id, "Placed up order");
                self.last_order_by_market.insert(market_id.clone(), Instant::now());

            } else if missing_down > Decimal::ZERO {
                let size = self.config.order_size.min(missing_down);
                let Some(price) = single_target_price(
                    down_ask,
                    up_ask,
                    self.config.target_total_cost,
                    self.config.maker_price_offset,
                ) else {
                    continue;
                };

                if size < min_size_for_notional(price) {
                    debug!(market_id, size = %size, "Down order too small");
                    continue;
                }

                let order_id = self.place_order(&down_token_id, price, size, &market_id, "down").await;

                if let Some(ref id) = order_id {
                    self.ledger.write().record_order_placed(
                        id.clone(),
                        market_id.clone(),
                        down_token_id.clone(),
                        MarketSide::Down,
                        size,
                        price,
                    );
                }

                info!(market_id, price = %price, size = %size, order = ?order_id, "Placed down order");
                self.last_order_by_market.insert(market_id.clone(), Instant::now());
            }
        }
    }

    fn cooldown_elapsed(&self, market_id: &str) -> bool {
        match self.last_order_by_market.get(market_id) {
            Some(t) => t.elapsed() >= Duration::from_secs(self.config.cooldown_secs),
            None => true,
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
        info!(
            market_id,
            price = %price,
            size = %size,
            dry_run = self.config.dry_run,
            label,
            "Placing order"
        );

        if self.config.dry_run {
            return Some(format!(
                "dry-run-{}-{}",
                label,
                Utc::now().timestamp_millis()
            ));
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
            .sign(&self.signer.clone().with_chain_id(Some(POLYGON)), signable)
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
            target = %self.config.shares_target_per_side,
            "Status update"
        );
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}

fn split_target_prices(
    up_ask: Decimal,
    down_ask: Decimal,
    target_total: Decimal,
    offset: Decimal,
) -> (Decimal, Decimal) {
    let denom = (up_ask + down_ask).max(dec!(0.001));
    let up_target = target_total * (up_ask / denom);
    let down_target = target_total - up_target;

    let up_price = maker_price(up_target, up_ask, offset);
    let down_price = maker_price(down_target, down_ask, offset);

    (up_price, down_price)
}

fn single_target_price(
    ask: Decimal,
    other_ask: Decimal,
    target_total: Decimal,
    offset: Decimal,
) -> Option<Decimal> {
    let target = target_total - other_ask;
    if target <= MIN_PRICE {
        return None;
    }
    Some(maker_price(target, ask, offset))
}

fn maker_price(target: Decimal, ask: Decimal, offset: Decimal) -> Decimal {
    let maker_cap = (ask - offset).max(MIN_PRICE).min(MAX_PRICE);
    let desired = target.min(maker_cap).max(MIN_PRICE).min(MAX_PRICE);
    desired.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::ToZero)
}

fn min_size_for_notional(price: Decimal) -> Decimal {
    if price <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let raw = MIN_ORDER_NOTIONAL / price;
    raw.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::AwayFromZero)
}
