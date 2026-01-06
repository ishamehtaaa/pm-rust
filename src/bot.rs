use polymarket_client_sdk::clob::types::SignatureType;
use crate::config::Config;
use crate::inventory::{MarketInventory, MarketOrders, RestingOrder};
use crate::market_cache::MarketCache;
use crate::models::{MarketState, Side, TradingPair};
use crate::ws::clob::ClobFeed;

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
const SCAN_LOOP_DELAY: Duration = Duration::from_millis(50);
const PRICE_POLL_INTERVAL: Duration = Duration::from_millis(200);
const POSITIONS_POLL_INTERVAL: Duration = Duration::from_secs(2);
const LADDER_REFRESH_INTERVAL: Duration = Duration::from_millis(200);
// Order pricing constants
const TICK_SIZE_DP: u32 = 2; // Polymarket uses 0.01 tick size (2 decimal places)
const MIN_PRICE: Decimal = dec!(0.01);
const MAX_PRICE: Decimal = dec!(0.99);
const LADDER_PRICE_STEP: Decimal = dec!(0.01);
const OPTIMISTIC_FILL_DELAY: Duration = Duration::from_secs(2);
const REBALANCE_IMBALANCE_SHARES: Decimal = dec!(10);
const EXIT_REBALANCE_SECS: f64 = 300.0;
const MAX_SPREAD_PER_SIDE: Decimal = dec!(0.05);
const INFORMED_PRICE_HIGH: Decimal = dec!(0.95);
const INFORMED_PRICE_LOW: Decimal = dec!(0.05);
const MAX_LADDER_STEP: Decimal = dec!(0.05);

#[derive(Debug, Default, Clone)]
struct MarketPnl {
    paired_shares: Decimal,
    cumulative_pnl: Decimal,
    last_pair_edge: Option<Decimal>,
}

/// Round price down to valid tick size for buy orders (conservative)
fn round_to_tick_buy(price: Decimal) -> Decimal {
    price.round_dp_with_strategy(TICK_SIZE_DP, rust_decimal::RoundingStrategy::ToZero)
}

fn clamp_price(price: Decimal) -> Decimal {
    round_to_tick_buy(price).max(MIN_PRICE).min(MAX_PRICE)
}

fn to_tick_price(price: Decimal) -> Decimal {
    clamp_price(price).round_dp(TICK_SIZE_DP)
}

fn to_lot_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero)
}

// Inventory types live in src/inventory.rs

pub struct HighFreqArbBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    clob_feed: ClobFeed,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    inventory: HashMap<String, MarketInventory>,
    orders: HashMap<String, MarketOrders>,
    token_to_market: HashMap<String, String>,
    token_to_side: HashMap<String, Side>,
    last_fill_at: HashMap<String, Instant>,
    pnl_by_market: HashMap<String, MarketPnl>,
    data_client: DataClient,
    user: Address,
    current_window_end: Option<DateTime<Utc>>,
    last_status_print: Instant,
    last_price_poll: Instant,
    last_positions_poll: Instant,
}

impl HighFreqArbBot {
    fn asset_name(&self, market_id: &str) -> String {
        self.markets
            .get(market_id)
            .map(|m| m.info.asset.to_uppercase())
            .unwrap_or_else(|| market_id.to_string())
    }

    fn token_id_for_side(&self, market_id: &str, side: Side) -> Option<String> {
        self.markets.get(market_id).map(|m| match side {
            Side::Up => m.info.up_token_id.clone(),
            Side::Down => m.info.down_token_id.clone(),
        })
    }
    #[instrument(skip(config), fields(target_assets = ?config.target_assets))]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        info!("Initializing HighFreqArbBot");

        let market_cache = MarketCache::new(config.target_assets.clone());
        debug!("Market cache initialized");

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

        let user = Address::from_str(&signer.address().to_string())?;

        Ok(Self {
            config,
            client: Arc::new(client),
            signer,
            market_cache,
            clob_feed: ClobFeed::new(),
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            inventory: HashMap::new(),
            orders: HashMap::new(),
            token_to_market: HashMap::new(),
            token_to_side: HashMap::new(),
            last_fill_at: HashMap::new(),
            pnl_by_market: HashMap::new(),
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
        let now = Instant::now();

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
            let prior = self.inventory.get(&market_id).cloned().unwrap_or_default();
            let up_increase = inventory.up_shares > prior.up_shares;
            let down_increase = inventory.down_shares > prior.down_shares;
            if up_increase || down_increase {
                self.last_fill_at.insert(market_id.clone(), now);
            }
            if up_increase ^ down_increase {
                warn!(
                    "[{}] single-leg fill detected | up_delta={} down_delta={}",
                    self.asset_name(&market_id),
                    (inventory.up_shares - prior.up_shares).round_dp(2),
                    (inventory.down_shares - prior.down_shares).round_dp(2)
                );
            }

            self.update_pnl(&market_id, &inventory, &prior);
            debug!(
                "[{}] positions snapshot | up: {} @ {:?} | down: {} @ {:?}",
                self.asset_name(&market_id),
                inventory.up_shares.round_dp(2),
                inventory.up_avg.map(|p| p.round_dp(3)),
                inventory.down_shares.round_dp(2),
                inventory.down_avg.map(|p| p.round_dp(3))
            );
            self.inventory.insert(market_id.clone(), inventory);
        }

        for market_id in self.markets.keys() {
            if !seen.contains(market_id) {
                self.inventory.insert(market_id.clone(), MarketInventory::default());
            }
        }
    }

    fn update_pnl(&mut self, market_id: &str, inventory: &MarketInventory, prior: &MarketInventory) {
        let Some(up_avg) = inventory.up_avg else { return };
        let Some(down_avg) = inventory.down_avg else { return };

        let paired_shares = inventory.up_shares.min(inventory.down_shares);
        let asset_name = self.asset_name(market_id);
        let tracker = self.pnl_by_market.entry(market_id.to_string()).or_default();
        if paired_shares <= tracker.paired_shares {
            return;
        }

        let combined_cost = up_avg + down_avg;
        let edge = dec!(1.0) - combined_cost;
        let new_pairs = paired_shares - tracker.paired_shares;
        let pnl_delta = edge * new_pairs;

        let prior_pnl = tracker.cumulative_pnl;
        tracker.cumulative_pnl += pnl_delta;
        tracker.paired_shares = paired_shares;
        tracker.last_pair_edge = Some(edge);

        info!(
            "[{}] paired fill | shares={} edge={} pnl_delta={} pnl_total={}",
            asset_name,
            new_pairs.round_dp(2),
            edge.round_dp(4),
            pnl_delta.round_dp(4),
            tracker.cumulative_pnl.round_dp(4)
        );

        if prior_pnl >= Decimal::ZERO && tracker.cumulative_pnl < Decimal::ZERO {
            warn!(
                "[{}] cumulative PnL negative | pnl_total={}",
                asset_name,
                tracker.cumulative_pnl.round_dp(4)
            );
        }

        if inventory.up_shares > prior.up_shares && inventory.down_shares > prior.down_shares {
            debug!(
                "[{}] both legs filled | up_delta={} down_delta={}",
                asset_name,
                (inventory.up_shares - prior.up_shares).round_dp(2),
                (inventory.down_shares - prior.down_shares).round_dp(2)
            );
        }
    }

    fn can_place_order(
        &self,
        market_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        other_ask: Decimal,
        allow_over_cost: bool,
    ) -> bool {
        if size <= Decimal::ZERO {
            return false;
        }

        let inv = self.inventory.get(market_id).cloned().unwrap_or_default();
        let max_shares = self.config.legging_config.max_shares_per_side;
        let max_total_cost = self.config.legging_config.max_total_cost;
        let cooldown = Duration::from_secs(self.config.legging_config.cooldown_secs);
        let pending = self.pending_shares(market_id, side);
        let optimistic = self.optimistic_shares(market_id, side);

        let effective_up = inv.up_shares + self.pending_shares(market_id, Side::Up)
            + self.optimistic_shares(market_id, Side::Up);
        let effective_down = inv.down_shares + self.pending_shares(market_id, Side::Down)
            + self.optimistic_shares(market_id, Side::Down);
        let effective_imbalance = effective_up - effective_down;
        if effective_imbalance >= REBALANCE_IMBALANCE_SHARES && side == Side::Up {
            debug!(
                "[{}] order blocked: hard lock on excess side (imbalance={})",
                self.asset_name(market_id),
                effective_imbalance.round_dp(2)
            );
            return false;
        }
        if effective_imbalance <= -REBALANCE_IMBALANCE_SHARES && side == Side::Down {
            debug!(
                "[{}] order blocked: hard lock on excess side (imbalance={})",
                self.asset_name(market_id),
                effective_imbalance.round_dp(2)
            );
            return false;
        }

        if cooldown > Duration::ZERO {
            if let Some(last) = self.last_fill_at.get(market_id) {
                if last.elapsed() < cooldown {
                    debug!(
                        "[{}] order blocked: cooldown {}ms not elapsed",
                        self.asset_name(market_id),
                        cooldown.as_millis()
                    );
                    return false;
                }
            }
        }

        let current = match side {
            Side::Up => inv.up_shares,
            Side::Down => inv.down_shares,
        };
        if current + pending + optimistic + size > max_shares {
            debug!(
                "[{}] order blocked: side={:?} token={} current={} pending={} optimistic={} size={} max={}",
                self.asset_name(market_id),
                side,
                self.token_id_for_side(market_id, side)
                    .unwrap_or_else(|| "-".to_string()),
                current.round_dp(2),
                pending.round_dp(2),
                optimistic.round_dp(2),
                size.round_dp(2),
                max_shares
            );
            return false;
        }

        let other_avg = match side {
            Side::Up => inv.down_avg,
            Side::Down => inv.up_avg,
        };
        let other_ref = other_avg.unwrap_or(other_ask);

        if !allow_over_cost && price + other_ref > max_total_cost {
            debug!(
                "[{}] order blocked: side={:?} token={} combined={} max_total_cost={}",
                self.asset_name(market_id),
                side,
                self.token_id_for_side(market_id, side)
                    .unwrap_or_else(|| "-".to_string()),
                (price + other_ref).round_dp(3),
                max_total_cost
            );
            return false;
        }

        true
    }

    fn pending_shares(&self, market_id: &str, side: Side) -> Decimal {
        self.orders
            .get(market_id)
            .map(|orders| orders.pending_shares(side))
            .unwrap_or(Decimal::ZERO)
    }

    fn optimistic_shares(&self, market_id: &str, side: Side) -> Decimal {
        self.orders
            .get(market_id)
            .map(|orders| orders.optimistic_shares(side, OPTIMISTIC_FILL_DELAY))
            .unwrap_or(Decimal::ZERO)
    }

    fn calculate_order_size(&self, market_id: &str, side: Side, edge: Decimal) -> Decimal {
        let base = self.config.arb_config.shares_per_side;
        let mut size = if edge >= dec!(0.05) {
            base * dec!(2.0)
        } else if edge >= dec!(0.03) {
            base * dec!(1.5)
        } else {
            base
        };

        let max_shares = self.config.legging_config.max_shares_per_side;
        let inv = self.inventory.get(market_id).cloned().unwrap_or_default();
        let pending = self.pending_shares(market_id, side);
        let optimistic = self.optimistic_shares(market_id, side);
        let current = match side {
            Side::Up => inv.up_shares,
            Side::Down => inv.down_shares,
        };

        let utilization = ((current + pending + optimistic) / max_shares).min(dec!(1.0));
        let scale = (dec!(1.0) - utilization).max(dec!(0.25));
        size *= scale;

        to_lot_size(size)
    }

    fn ladder_step_for(&self, ask: Decimal, bid: Option<Decimal>) -> Decimal {
        let spread = bid.map(|b| (ask - b).max(Decimal::ZERO)).unwrap_or(LADDER_PRICE_STEP);
        (spread * dec!(0.25))
            .max(LADDER_PRICE_STEP)
            .min(MAX_LADDER_STEP)
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

        let (up_ask, down_ask, up_bid, down_bid, remaining_secs) = {
            let state = match self.markets.get(market_id) {
                Some(s) => s,
                None => return,
            };
            let pair = state.pair.read();
            let remaining_secs = state.remaining_seconds(Utc::now());
            let (Some(up_ask), Some(down_ask)) = (pair.latest_up_ask(), pair.latest_down_ask()) else {
                debug!(
                    "[{}] ladder skipped: missing WS asks",
                    self.asset_name(market_id)
                );
                drop(pair);
                self.cancel_all_orders(market_id).await;
                return;
            };
            (up_ask, down_ask, pair.rest_up_bid, pair.rest_down_bid, remaining_secs)
        };

        let inv = self.inventory.get(market_id).cloned().unwrap_or_default();
        let max_total_cost = self.config.legging_config.max_total_cost;
        let max_shares_per_side = self.config.legging_config.max_shares_per_side;
        let pending_up = self.pending_shares(market_id, Side::Up);
        let pending_down = self.pending_shares(market_id, Side::Down);
        let optimistic_up = self.optimistic_shares(market_id, Side::Up);
        let optimistic_down = self.optimistic_shares(market_id, Side::Down);
        let optimistic_up = self.optimistic_shares(market_id, Side::Up);
        let optimistic_down = self.optimistic_shares(market_id, Side::Down);
        let effective_up = inv.up_shares + optimistic_up;
        let effective_down = inv.down_shares + optimistic_down;
        let remaining_up =
            (max_shares_per_side - (effective_up + pending_up)).max(Decimal::ZERO);
        let remaining_down =
            (max_shares_per_side - (effective_down + pending_down)).max(Decimal::ZERO);
        let effective_imbalance = effective_up - effective_down;
        let abs_imbalance = effective_imbalance.abs();
        let rebalance_only_side = if effective_imbalance >= REBALANCE_IMBALANCE_SHARES {
            Some(Side::Down)
        } else if effective_imbalance <= -REBALANCE_IMBALANCE_SHARES {
            Some(Side::Up)
        } else {
            None
        };

        let exit_mode = remaining_secs <= EXIT_REBALANCE_SECS && abs_imbalance >= REBALANCE_IMBALANCE_SHARES;

        if rebalance_only_side == Some(Side::Down) {
            self.replace_side_orders(market_id, Side::Up, &[]).await;
        } else if rebalance_only_side == Some(Side::Up) {
            self.replace_side_orders(market_id, Side::Down, &[]).await;
        }

        if up_ask >= INFORMED_PRICE_HIGH
            || down_ask >= INFORMED_PRICE_HIGH
            || up_ask <= INFORMED_PRICE_LOW
            || down_ask <= INFORMED_PRICE_LOW
        {
            debug!(
                "[{}] ladder skipped: informed pricing band (up_ask={}, down_ask={})",
                self.asset_name(market_id),
                up_ask.round_dp(3),
                down_ask.round_dp(3)
            );
            self.cancel_all_orders(market_id).await;
            return;
        }

        if let (Some(up_bid), Some(down_bid)) = (up_bid, down_bid) {
            let up_spread = (up_ask - up_bid).max(Decimal::ZERO);
            let down_spread = (down_ask - down_bid).max(Decimal::ZERO);
            if up_spread > MAX_SPREAD_PER_SIDE || down_spread > MAX_SPREAD_PER_SIDE {
                debug!(
                    "[{}] ladder skipped: wide spread (up_spread={}, down_spread={})",
                    self.asset_name(market_id),
                    up_spread.round_dp(3),
                    down_spread.round_dp(3)
                );
                self.cancel_all_orders(market_id).await;
                return;
            }
        }

        if remaining_up <= Decimal::ZERO && remaining_down <= Decimal::ZERO {
            debug!(
                "[{}] ladder skipped: max_shares_per_side reached (up={}, down={})",
                self.asset_name(market_id),
                effective_up.round_dp(2),
                effective_down.round_dp(2)
            );
            self.cancel_all_orders(market_id).await;
            return;
        }

        let effective_max_total_cost = if exit_mode {
            dec!(1.00)
        } else {
            max_total_cost
        };
        let mut up_base = to_tick_price(effective_max_total_cost - down_ask);
        let mut down_base = to_tick_price(effective_max_total_cost - up_ask);
        if rebalance_only_side == Some(Side::Up) {
            up_base = to_tick_price(up_ask);
        } else if rebalance_only_side == Some(Side::Down) {
            down_base = to_tick_price(down_ask);
        }
        let edge = (effective_max_total_cost - (up_ask + down_ask)).max(Decimal::ZERO);

        let levels = self.config.legging_config.max_live_orders_per_token.max(1);
        let up_step = self.ladder_step_for(up_ask, up_bid);
        let down_step = self.ladder_step_for(down_ask, down_bid);
        let base_up_size = self.calculate_order_size(market_id, Side::Up, edge);
        let base_down_size = self.calculate_order_size(market_id, Side::Down, edge);

        let mut new_up_orders = Vec::new();
        let mut new_down_orders = Vec::new();

        let mut remaining_up_size = remaining_up;
        if remaining_up_size > Decimal::ZERO && rebalance_only_side != Some(Side::Down) {
            for i in 0..levels {
                if remaining_up_size <= Decimal::ZERO {
                    break;
                }
                let price = to_tick_price(up_base - up_step * Decimal::from(i as u32));
                let size = to_lot_size(base_up_size.min(remaining_up_size));
                let allow_over_cost = exit_mode;
                if !self.can_place_order(market_id, Side::Up, price, size, down_ask, allow_over_cost) {
                    continue;
                }
                new_up_orders.push((price, size));
                remaining_up_size -= size;
            }
        }

        let mut remaining_down_size = remaining_down;
        if remaining_down_size > Decimal::ZERO && rebalance_only_side != Some(Side::Up) {
            for i in 0..levels {
                if remaining_down_size <= Decimal::ZERO {
                    break;
                }
                let price = to_tick_price(down_base - down_step * Decimal::from(i as u32));
                let size = to_lot_size(base_down_size.min(remaining_down_size));
                let allow_over_cost = exit_mode;
                if !self.can_place_order(market_id, Side::Down, price, size, up_ask, allow_over_cost) {
                    continue;
                }
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
                    placed_at: Instant::now(),
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
        let asset = self.asset_name(market_id);

        if price < MIN_PRICE || price > MAX_PRICE {
            warn!(
                "[{}] ladder order skipped: price out of bounds {}",
                asset,
                price
            );
            return None;
        }
        let size = to_lot_size(size);
        if size <= Decimal::ZERO {
            warn!(
                "[{}] ladder order skipped: size <= 0 ({})",
                asset,
                size
            );
            return None;
        }

        let price = to_tick_price(price);
        let price_str = format!("{:.2}", price);
        let size_str = format!("{:.2}", size);
        let poly_price = match PolyDecimal::try_from(price_str.as_str()) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    "[{}] ladder order skipped: invalid price {} ({})",
                    asset,
                    price_str,
                    e
                );
                return None;
            }
        };
        let poly_size = match PolyDecimal::try_from(size_str.as_str()) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    "[{}] ladder order skipped: invalid size {} ({})",
                    asset,
                    size_str,
                    e
                );
                return None;
            }
        };

        if self.config.dry_run {
            let oid = format!("dry-{}-{}", token_id, Utc::now().timestamp_millis());
            debug!(
                "[{}] ladder order placed (dry-run) side={:?} price={} size={} order_id={}",
                asset,
                side,
                price,
                size,
                oid
            );
            return Some(oid);
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
            .map_err(|e| {
                warn!(
                    "[{}] ladder order build failed side={:?} price={} size={} err={}",
                    asset,
                    side,
                    price_str,
                    size_str,
                    e
                );
                e
            })
            .ok()?;

        let signed = self
            .client
            .sign(&self.signer.clone().with_chain_id(Some(POLYGON)), signable)
            .await
            .map_err(|e| {
                warn!(
                    "[{}] ladder order sign failed side={:?} price={} size={} err={}",
                    asset,
                    side,
                    price_str,
                    size_str,
                    e
                );
                e
            })
            .ok()?;

        let response = match self.client.post_order(signed).await {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    "[{}] ladder order post failed side={:?} price={} size={} err={}",
                    asset,
                    side,
                    price_str,
                    size_str,
                    e
                );
                return None;
            }
        };

        if let Some(ref msg) = response.error_msg {
            if !msg.is_empty() {
                warn!(
                    "[{}] ladder order rejected side={:?} price={} size={} err={}",
                    asset,
                    side,
                    price_str,
                    size_str,
                    msg
                );
                return None;
            }
        }

        debug!(
            "[{}] ladder order placed side={:?} price={} size={} order_id={}",
            asset,
            side,
            price_str,
            size_str,
            response.order_id
        );
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
            let pending_up = self.pending_shares(market_id, Side::Up);
            let pending_down = self.pending_shares(market_id, Side::Down);
            let optimistic_up = self.optimistic_shares(market_id, Side::Up);
            let optimistic_down = self.optimistic_shares(market_id, Side::Down);
            let up_ask = pair
                .rest_up_ask
                .or_else(|| self.clob_feed.ws_price(&pair.up_token_id).and_then(|(_, a)| a));
            let down_ask = pair
                .rest_down_ask
                .or_else(|| self.clob_feed.ws_price(&pair.down_token_id).and_then(|(_, a)| a));
            let combined = match (inv.up_avg, inv.down_avg) {
                (Some(u), Some(d)) => Some(u + d),
                _ => None,
            };
            let edge = combined.map(|c| dec!(1.0) - c);
            let combined_ask = match (up_ask, down_ask) {
                (Some(u), Some(d)) => Some(u + d),
                _ => None,
            };
            let ladder_edge = combined_ask.map(|c| self.config.legging_config.max_total_cost - c);
            let ladder_up_base = down_ask
                .map(|d| to_tick_price(self.config.legging_config.max_total_cost - d));
            let ladder_down_base = up_ask
                .map(|u| to_tick_price(self.config.legging_config.max_total_cost - u));
            let pnl_total = self
                .pnl_by_market
                .get(market_id)
                .map(|p| p.cumulative_pnl)
                .unwrap_or(Decimal::ZERO);

            info!(
                asset = %state.info.asset.to_uppercase(),
                elapsed_pct = format!("{:.1}%", state.elapsed_pct(now) * 100.0),
                up_ask = up_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                down_ask = down_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                up_shares = %inv.up_shares.round_dp(2),
                down_shares = %inv.down_shares.round_dp(2),
                up_pending = %pending_up.round_dp(2),
                down_pending = %pending_down.round_dp(2),
                up_optimistic = %optimistic_up.round_dp(2),
                down_optimistic = %optimistic_down.round_dp(2),
                combined = combined.map(|c| format!("{:.3}", c)).unwrap_or_else(|| "---".to_string()),
                edge = edge.map(|e| format!("{:.3}", e)).unwrap_or_else(|| "---".to_string()),
                combined_ask = combined_ask.map(|c| format!("{:.3}", c)).unwrap_or_else(|| "---".to_string()),
                target_cost = %self.config.legging_config.max_total_cost.round_dp(3),
                ladder_edge = ladder_edge.map(|e| format!("{:.3}", e)).unwrap_or_else(|| "---".to_string()),
                ladder_up = ladder_up_base.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                ladder_down = ladder_down_base.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".to_string()),
                pnl_total = %pnl_total.round_dp(4),
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
            .filter(|m| m.start_time <= now && m.end_time > now)
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
        self.clob_feed.stop();
        self.clob_feed.clear();

        let mut tokens = Vec::new();
        for (asset, info) in selected {
            let market_id = info.id.clone();
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            // Gamma start_time is often earlier than the 15m window we trade.
            let start_time = info.end_time - chrono::TimeDelta::minutes(15);
            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol: String::new(),
                start_time,
                end_time: info.end_time,
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
            tokens.push((info.up_token_id.clone(), Side::Up));
            tokens.push((info.down_token_id.clone(), Side::Down));
        }

        self.clob_feed.set_pairs(tokens);
        self.clob_feed.start_ws_only();

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

        for (market_id, _, up_token_id, down_token_id) in &market_info {
            if let Some((bid, ask)) = self.clob_feed.ws_price(up_token_id) {
                if let Some(pair) = self.trading_pairs.get(market_id) {
                    let mut p = pair.write();
                    p.rest_up_bid = bid;
                    p.rest_up_ask = ask;
                    p.last_rest_update_ms = crate::models::now_ms();
                }
            }

            if let Some((bid, ask)) = self.clob_feed.ws_price(down_token_id) {
                if let Some(pair) = self.trading_pairs.get(market_id) {
                    let mut p = pair.write();
                    p.rest_down_bid = bid;
                    p.rest_down_ask = ask;
                    p.last_rest_update_ms = crate::models::now_ms();
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
