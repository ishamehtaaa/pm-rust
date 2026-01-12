use crate::config::Config;
use crate::clob_api::fetch_token_balances;
use crate::ladder::{LadderConfig, LadderEngine, LadderOrder, LadderState};
use crate::market_cache::MarketCache;
use crate::models::{MarketState, TradingPair};
use crate::poller::{InventoryLedger, MarketSide, MarketTokens, spawn_order_poller};
use crate::price_feed::{PriceCache, spawn_price_feed};
use chrono::Timelike;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::Utc;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::{Side as ClobSide, SignatureType, SignedOrder};
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::{Address, Decimal as PolyDecimal};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, instrument, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const LOOP_DELAY: Duration = Duration::from_millis(200);

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

    ledger: Arc<RwLock<InventoryLedger>>,
    price_cache: Arc<RwLock<PriceCache>>,
    _order_poller: tokio::task::JoinHandle<()>,
    _price_feed: Option<tokio::task::JoinHandle<()>>,
    active_market_tokens: Arc<RwLock<Vec<MarketTokens>>>,

    ladder_engine: LadderEngine,
    ladder_state: LadderState,
}

impl SimpleBot {
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing SimpleBot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        let price_cache = Arc::new(RwLock::new(PriceCache::default()));

        let ladder_engine = LadderEngine::new(LadderConfig::default());
        let ladder_state = LadderState::default();

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
        let active_market_tokens = Arc::new(RwLock::new(Vec::new()));

        let poller = spawn_order_poller(
            client.clone(),
            ledger.clone(),
            active_market_tokens.clone(),
            Duration::from_millis(3000),
        );

        Ok(Self {
            config,
            client,
            signer,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            last_market_refresh: Instant::now(),
            last_order_by_market: HashMap::new(),
            active_market_tokens,
            ledger,
            _order_poller: poller,
            price_cache,
            _price_feed: None,
            ladder_engine,
            ladder_state,
        })
    }

    pub async fn run(&mut self) {
        self.discover_markets().await;
        info!("Entering main loop...");

        loop {
            if self.should_refresh_markets() {
                self.discover_markets().await;
                self.last_market_refresh = Instant::now();
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

        // Update active market tokens after processing all markets
        {
            let mut tokens = self.active_market_tokens.write();
            *tokens = self
                .markets
                .values()
                .map(|m| MarketTokens {
                    market_id: m.info.id.clone(),
                    up_token_id: m.info.up_token_id.clone(),
                    down_token_id: m.info.down_token_id.clone(),
                })
                .collect();
        }

        let token_ids: Vec<String> = self
            .markets
            .values()
            .flat_map(|m| vec![m.info.up_token_id.clone(), m.info.down_token_id.clone()])
            .collect();

        if let Some(handle) = self._price_feed.take() {
            handle.abort();
        }

        for (market_id, state) in &self.markets {
            match fetch_token_balances(
                &self.client,
                &state.info.up_token_id,
                &state.info.down_token_id,
            )
            .await
            {
                Ok((up_bal, down_bal)) => {
                    self.ledger
                        .write()
                        .set_initial_position(market_id.clone(), up_bal, down_bal);
                    info!(
                        market_id,
                        up_shares = %up_bal,
                        down_shares = %down_bal,
                        "Position initialized"
                    );
                }
                Err(e) => {
                    warn!(market_id, error = %e, "Failed to fetch initial position");
                }
            }
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
    fn should_refresh_markets(&self) -> bool {
        if self.last_market_refresh.elapsed() < Duration::from_secs(30) {
            return false;
        }

        let now = Utc::now();
        let total_seconds = now.minute() * 60 + now.second();

        /* The seconds into each 15 min window where we must refresh. */
        let position_in_window = total_seconds % 900;

        (840..870).contains(&position_in_window)
    }

    fn cooldown_elapsed(&self, market_id: &str) -> bool {
        match self.last_order_by_market.get(market_id) {
            Some(t) => t.elapsed() >= Duration::from_secs(self.config.cooldown_secs),
            None => true,
        }
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    async fn scan(&mut self) {
        let market_ids: Vec<String> = self.markets.keys().cloned().collect();

        for market_id in market_ids {
            let (up_token_id, down_token_id) = match self.markets.get(&market_id) {
                Some(s) => (s.info.up_token_id.clone(), s.info.down_token_id.clone()),
                None => continue,
            };

            // Get prices from WebSocket cache
            let (up_ask, down_ask) = {
                let cache = self.price_cache.read();
                match (cache.get(&up_token_id), cache.get(&down_token_id)) {
                    (Some((_, up_ask)), Some((_, down_ask))) => (up_ask, down_ask),
                    _ => {
                        debug!(market_id, "Missing price data");
                        continue;
                    }
                }
            };
            debug!("UP: {}, DOWN: {}", up_ask, down_ask);

            // Get position, pending, and open orders from ledger (single source of truth)
            let (pos, pending_up, pending_down, open_orders) = {
                let ledger = self.ledger.read();
                let pos = ledger.confirmed_position(&market_id);
                let (pending_up, pending_down) = ledger.pending_for_market(&market_id);
                let open_orders = ledger.open_orders_for_market(&market_id);
                (pos, pending_up, pending_down, open_orders)
            };

            // Check if we need to re-ladder
            let should_reladder = self.ladder_state.should_reladder(
                &market_id,
                up_ask,
                down_ask,
                self.ladder_engine.config().reladder_threshold,
            );

            if !should_reladder && !self.cooldown_elapsed(&market_id) {
                continue;
            }

            // Compute ladder plan
            let plan = self.ladder_engine.compute_ladder(
                up_ask,
                down_ask,
                &pos,
                pending_up,
                pending_down,
                &open_orders,
            );

            // Nothing to do
            if plan.cancellations.is_empty() && plan.orders.is_empty() {
                continue;
            }

            // Cancel stale orders first
            if !plan.cancellations.is_empty() {
                match self.cancel_orders(&plan.cancellations).await {
                    Ok(cancelled) => {
                        info!(market_id, count = cancelled.len(), "Cancelled orders");
                        self.ledger.write().mark_orders_cancelled(&cancelled);
                    }
                    Err(e) => {
                        error!(market_id, error = %e, "Failed to cancel orders");
                    }
                }
            }

            // Place new ladder orders
            if !plan.orders.is_empty() {
                let signed_orders = self
                    .build_signed_orders(&plan.orders, &up_token_id, &down_token_id)
                    .await;

                if !signed_orders.is_empty() {
                    match self.client.post_orders(signed_orders).await {
                        Ok(responses) => {
                            let mut placed = 0;
                            for (resp, order) in responses.iter().zip(plan.orders.iter()) {
                                if resp
                                    .error_msg
                                    .as_ref()
                                    .map(|s| s.is_empty())
                                    .unwrap_or(true)
                                {
                                    let token_id = match order.side {
                                        MarketSide::Up => &up_token_id,
                                        MarketSide::Down => &down_token_id,
                                    };
                                    self.ledger.write().record_order_placed(
                                        resp.order_id.clone(),
                                        market_id.clone(),
                                        token_id.clone(),
                                        order.side,
                                        order.size,
                                        order.price,
                                    );
                                    placed += 1;
                                } else {
                                    warn!(
                                        market_id,
                                        order_id = %resp.order_id,
                                        error = ?resp.error_msg,
                                        "Order rejected"
                                    );
                                }
                            }

                            info!(
                                market_id,
                                up_ask = %up_ask,
                                down_ask = %down_ask,
                                cancelled = plan.cancellations.len(),
                                placed,
                                "Ladder updated"
                            );

                            self.ladder_state
                                .record_ladder(market_id.clone(), up_ask, down_ask);
                        }
                        Err(e) => {
                            error!(market_id, error = %e, "Batch order submission failed");
                        }
                    }
                }
            }

            self.last_order_by_market
                .insert(market_id.clone(), Instant::now());
        }
    }

    async fn build_signed_orders(
        &self,
        orders: &[LadderOrder],
        up_token_id: &str,
        down_token_id: &str,
    ) -> Vec<SignedOrder> {
        let mut signed = Vec::with_capacity(orders.len());

        for order in orders {
            let token_id = match order.side {
                MarketSide::Up => up_token_id,
                MarketSide::Down => down_token_id,
            };

            let poly_price = match PolyDecimal::try_from(order.price.to_string().as_str()) {
                Ok(p) => p,
                Err(e) => {
                    error!(error = %e, "Invalid price format");
                    continue;
                }
            };

            let poly_size = match PolyDecimal::try_from(order.size.to_string().as_str()) {
                Ok(s) => s,
                Err(e) => {
                    error!(error = %e, "Invalid size format");
                    continue;
                }
            };

            info!(
                "Placing order for {} shares @ {} for side {:?}",
                poly_size, poly_price, order.side
            );

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
                    error!(error = %e, "Failed to build order");
                    continue;
                }
            };

            match self
                .client
                .sign(&self.signer.clone().with_chain_id(Some(POLYGON)), signable)
                .await
            {
                Ok(s) => signed.push(s),
                Err(e) => {
                    error!(error = %e, "Failed to sign order");
                }
            }
        }

        signed
    }

    async fn cancel_orders(&self, order_ids: &[String]) -> anyhow::Result<Vec<String>> {
        if order_ids.is_empty() {
            return Ok(Vec::new());
        }

        if self.config.dry_run {
            info!(count = order_ids.len(), "Dry run: would cancel orders");
            return Ok(order_ids.to_vec());
        }

        /* The SDK expects a list of order IDs to cancel */
        let order_id_refs: Vec<&str> = order_ids.iter().map(|s| s.as_str()).collect();

        match self.client.cancel_orders(&order_id_refs).await {
            Ok(response) => {
                info!(
                    requested = order_ids.len(),
                    cancelled = response.canceled.len(),
                    not_cancelled = response.not_canceled.len(),
                    "Batch cancel complete"
                );

                Ok(response.canceled)
            }
            Err(e) => {
                error!(error = %e, "Batch cancel failed");
                Err(e.into())
            }
        }
    }
}
