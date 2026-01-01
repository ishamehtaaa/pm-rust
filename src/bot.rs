use crate::chainlink::ChainlinkClient;
use crate::clob_client::ClobClient;
use crate::config::{Config, ASSETS_BY_NAME};
use crate::edge::{EdgeDetector, EdgeSignal, MarketSnapshot, Side as EdgeSide};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, Side, TradingPair};
use crate::ws::ClobFeed;
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

pub struct HighFreqArbBot {
    config: Config,
    clob_client: ClobClient,
    market_cache: MarketCache,
    chainlink: ChainlinkClient,
    edge_detector: EdgeDetector,

    clob_feed: ClobFeed,

    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    positions: HashMap<String, Position>,

    current_window_end: Option<DateTime<Utc>>,
    last_status_print: Instant,
    last_chainlink_update: Instant,
    chainlink_prices: HashMap<String, Decimal>,  // asset -> current price
}

#[derive(Debug, Clone)]
pub struct Position {
    pub market_id: String,
    pub asset: String,
    pub side: Side,
    pub order_id: Option<String>,
    pub price: Decimal,
    pub size: Decimal,
    pub created_at: DateTime<Utc>,
}

impl HighFreqArbBot {
    pub fn new(config: Config, clob_client: ClobClient) -> Self {
        let market_cache = MarketCache::new(config.target_assets.clone());
        let clob_feed = ClobFeed::new();

        Self {
            config,
            clob_client,
            market_cache,
            chainlink: ChainlinkClient::new(),
            edge_detector: EdgeDetector::default(),
            clob_feed,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            positions: HashMap::new(),
            current_window_end: None,
            last_status_print: Instant::now(),
            last_chainlink_update: Instant::now(),
            chainlink_prices: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        info!("Starting bot main loop");

        if !self.markets.is_empty() {
            let pairs = self.get_trading_pairs();
            self.clob_feed.set_pairs(pairs);
        }

        self.clob_feed.start();

        loop {
            let now = Utc::now();

            if self.should_rotate_markets(now) {
                info!("Market window ended, rotating...");
                self.positions.clear();
                self.discover_markets().await;

                let pairs = self.get_trading_pairs();
                self.clob_feed.set_pairs(pairs);

                if self.markets.is_empty() {
                    warn!("No markets available, waiting 30s...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }

            // Update Chainlink prices every 2 seconds
            if self.last_chainlink_update.elapsed() > Duration::from_secs(2) {
                self.update_chainlink_prices().await;
                self.last_chainlink_update = Instant::now();
            }

            // Print status every 5 seconds
            if self.last_status_print.elapsed() > Duration::from_secs(5) {
                self.print_status();
                self.last_status_print = Instant::now();
            }

            // Scan and execute on edges
            let signals = self.scan_for_edges();
            for signal in &signals {
                info!("EDGE: {}", signal);
            }
            for signal in signals {
                self.execute_entry(&signal).await;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn update_chainlink_prices(&mut self) {
        for state in self.markets.values() {
            match self.chainlink.get_latest_price(&state.info.asset).await {
                Ok(price_data) => {
                    self.chainlink_prices.insert(state.info.asset.clone(), price_data.price);
                }
                Err(e) => {
                    warn!("Failed to get Chainlink price for {}: {}", state.info.asset, e);
                }
            }
        }
    }

    fn print_status(&self) {
        let now = Utc::now();
        let now_ms = crate::models::now_ms();

        println!("\n{}", "=".repeat(80));
        println!("STATUS @ {}", now.with_timezone(&New_York).format("%H:%M:%S"));
        println!("{}", "-".repeat(80));

        if self.markets.is_empty() {
            println!("No active markets");
            return;
        }

        for (market_id, state) in &self.markets {
            let pair = state.pair.read();
            let duration_secs = 900.0;
            let remaining = state.remaining_seconds(now);
            let elapsed_pct = ((duration_secs - remaining) / duration_secs * 100.0).clamp(0.0, 100.0);
            let remaining_secs = state.remaining_seconds(now);

            // Chainlink prices
            let chainlink_current = self.chainlink_prices.get(&state.info.asset).copied();
            let chainlink_open = state.binance_open_price; // renamed field, still holds open price

            // Calculate move %
            let move_pct = match (chainlink_open, chainlink_current) {
                (Some(open), Some(cur)) if !open.is_zero() => {
                    ((cur - open) / open) * dec!(100)
                }
                _ => Decimal::ZERO,
            };

            // Price staleness
            let stale_ms = if pair.last_update_ms > 0 {
                now_ms - pair.last_update_ms
            } else {
                -1
            };

            let position_marker = if self.positions.contains_key(market_id) { " [POS]" } else { "" };

            println!(
                "{:<10} | elapsed: {:5.1}% | remaining: {:5.0}s{}",
                state.info.asset.to_uppercase(),
                elapsed_pct,
                remaining_secs,
                position_marker
            );

            println!(
                "           | Chainlink: open={} cur={} move={:+.3}%",
                chainlink_open.map(|p| format!("{:.2}", p)).unwrap_or_else(|| "---".into()),
                chainlink_current.map(|p| format!("{:.2}", p)).unwrap_or_else(|| "---".into()),
                move_pct
            );

            println!(
                "           | PM: UP_ask={} DOWN_ask={} combined={} (stale: {}ms)",
                pair.up_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".into()),
                pair.down_ask.map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".into()),
                pair.combined_ask().map(|p| format!("{:.3}", p)).unwrap_or_else(|| "---".into()),
                if stale_ms >= 0 { stale_ms.to_string() } else { "never".into() }
            );

            // Check for edge
            if let (Some(open), Some(cur)) = (chainlink_open, chainlink_current) {
                let snapshot = MarketSnapshot {
                    asset: state.info.asset.clone(),
                    market_id: market_id.clone(),
                    open_price: open,
                    current_price: cur,
                    pm_up_ask: pair.up_ask.unwrap_or_default(),
                    pm_down_ask: pair.down_ask.unwrap_or_default(),
                    elapsed_pct: elapsed_pct / 100.0,
                };

                if let Some(signal) = self.edge_detector.analyze(&snapshot) {
                    println!(
                        "           | EDGE: {:?} fair={:.3} ask={:.3} edge={:.3}",
                        signal.side,
                        signal.fair_value,
                        signal.market_ask,
                        signal.edge
                    );
                }
            }

            println!();
        }

        println!("Positions: {}", self.positions.len());
        for (_, pos) in &self.positions {
            println!("  {} {:?} @ {} ({})", pos.asset, pos.side, pos.price, pos.order_id.as_deref().unwrap_or("?"));
        }
        println!("{}", "=".repeat(80));
    }

    fn scan_for_edges(&self) -> Vec<EdgeSignal> {
        let now = Utc::now();
        let mut signals = Vec::new();

        for (market_id, state) in &self.markets {
            if self.positions.contains_key(market_id) {
                continue;
            }

            if now >= state.end_time {
                continue;
            }

            let open_price = match state.binance_open_price {
                Some(p) => p,
                None => continue,
            };

            let current_price = match self.chainlink_prices.get(&state.info.asset) {
                Some(&p) => p,
                None => continue,
            };

            let (up_ask, down_ask) = {
                let pair = state.pair.read();
                match (pair.up_ask, pair.down_ask) {
                    (Some(u), Some(d)) => (u, d),
                    _ => continue,
                }
            };

            let duration_secs = 900.0;
            let remaining = state.remaining_seconds(now);
            let elapsed_pct = ((duration_secs - remaining) / duration_secs).clamp(0.0, 1.0);

            let snapshot = MarketSnapshot {
                asset: state.info.asset.clone(),
                market_id: market_id.clone(),
                open_price: open_price,
                current_price: current_price,
                pm_up_ask: up_ask,
                pm_down_ask: down_ask,
                elapsed_pct,
            };

            if let Some(signal) = self.edge_detector.analyze(&snapshot) {
                signals.push(signal);
            }
        }

        signals
    }

    async fn execute_entry(&mut self, signal: &EdgeSignal) {
        let state = match self.markets.get(&signal.market_id) {
            Some(s) => s,
            None => return,
        };

        let (token_id, side) = match signal.side {
            EdgeSide::Up => (&state.info.up_token_id, Side::Up),
            EdgeSide::Down => (&state.info.down_token_id, Side::Down),
        };

        // Place order 1 tick below ask to sit on book (avoid marketable order issues)
        // Polymarket uses 0.01 tick size
        let limit_price = signal.market_ask - dec!(0.01);
        
        let size = self.config.arb_config.shares_per_side;
        let total_cost = limit_price * size;

        // Polymarket minimum marketable order is $1
        if total_cost < dec!(1.0) {
            warn!(
                "{} order too small: ${} (min $1), skipping",
                signal.asset, total_cost
            );
            return;
        }

        info!(
            "ENTRY: {} {:?} | price={} size={} total_cost=${} | fair={:.3} edge={:.3} move={:+.2}%",
            signal.asset, side, limit_price, size, total_cost,
            signal.fair_value, signal.edge,
            signal.price_move_pct * dec!(100)
        );

        let result = self.clob_client.place_limit_order(token_id, limit_price, size).await;

        if result.placed() {
            info!("{} {:?} order placed: {}", signal.asset, side, result.order_id.as_deref().unwrap_or("?"));

            self.positions.insert(signal.market_id.clone(), Position {
                market_id: signal.market_id.clone(),
                asset: signal.asset.clone(),
                side,
                order_id: result.order_id,
                price: limit_price,
                size,
                created_at: Utc::now(),
            });
        } else {
            warn!("{} {:?} order FAILED: {}", signal.asset, side, result.error.as_deref().unwrap_or("unknown"));
        }
    }

    pub async fn discover_markets(&mut self) {
        let now = Utc::now();

        let all_markets = match self.market_cache.get_markets(now).await {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to fetch markets: {}", e);
                return;
            }
        };

        let mut valid: Vec<MarketInfo> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

        valid.sort_by_key(|m| m.end_time);

        let mut selected: HashMap<String, MarketInfo> = HashMap::new();
        for market in valid {
            if !selected.contains_key(&market.asset) {
                selected.insert(market.asset.clone(), market);
            }
        }

        if selected.is_empty() {
            warn!("No valid markets found");
            self.markets.clear();
            self.trading_pairs.clear();
            self.current_window_end = None;
            return;
        }

        self.markets.clear();
        self.trading_pairs.clear();

        for (asset, info) in &selected {
            let binance_symbol = ASSETS_BY_NAME
                .get(asset)
                .map(|a| a.binance.clone())
                .unwrap_or_default();

            let pair = Arc::new(RwLock::new(info.to_trading_pair()));

            // Fetch open price from Chainlink at market start time
            let chainlink_open_price = match self.chainlink.get_price_at(asset, info.start_time).await {
                Ok(price_data) => {
                    info!(
                        "{} Chainlink open: {} @ {} (target: {})",
                        asset, price_data.price, price_data.timestamp, info.start_time
                    );
                    Some(price_data.price)
                }
                Err(e) => {
                    warn!("{} Chainlink open price error: {}", asset, e);
                    // Fallback to latest
                    match self.chainlink.get_latest_price(asset).await {
                        Ok(p) => Some(p.price),
                        Err(_) => None,
                    }
                }
            };

            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol,
                start_time: info.start_time,
                end_time: info.end_time,
                binance_open_price: chainlink_open_price,  // reusing field name
            };

            self.markets.insert(info.id.clone(), state);
            self.trading_pairs.insert(info.id.clone(), pair);
        }

        self.current_window_end = self.markets.values().map(|s| s.end_time).min();

        let assets: Vec<&str> = self.markets.values().map(|s| s.info.asset.as_str()).collect();
        if let Some(first) = self.markets.values().next() {
            let start_est = first.start_time.with_timezone(&New_York);
            let end_est = first.end_time.with_timezone(&New_York);
            info!("Markets: {:?} | {}-{} EST", assets, start_est.format("%H:%M"), end_est.format("%H:%M"));
        }
    }

    fn should_rotate_markets(&self, now: DateTime<Utc>) -> bool {
        match self.current_window_end {
            Some(end) => now >= end,
            None => true,
        }
    }

    fn get_trading_pairs(&self) -> Vec<Arc<RwLock<TradingPair>>> {
        self.trading_pairs.values().cloned().collect()
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}