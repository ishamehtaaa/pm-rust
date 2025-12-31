use crate::binance_rest::BinanceRest;
use crate::clob_client::{ClobClient, PriceSide};
use crate::config::{Config, ASSETS_BY_NAME};
use crate::edge::{Direction, EdgeDetector, EdgeSignal, EdgeStrength};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, Side, TradingPair};
use crate::ws::{BinanceFeed, ClobFeed};
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub struct HighFreqArbBot {
    config: Config,
    clob_client: ClobClient,
    market_cache: MarketCache,
    binance_rest: BinanceRest,
    edge_detector: EdgeDetector,

    // WebSocket feeds
    binance_feed: BinanceFeed,
    clob_feed: ClobFeed,

    // market_id -> MarketState
    markets: HashMap<String, MarketState>,

    // Shared references to trading pairs for websocket feeds to update
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,

    // Track positions to avoid double-entry
    positions: HashMap<String, Position>,

    current_window_end: Option<DateTime<Utc>>,
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
    pub entry_reason: EntryReason,
}

#[derive(Debug, Clone)]
pub enum EntryReason {
    CombinedArb { combined_cost: Decimal },
    EdgeSignal { direction: Direction, strength: EdgeStrength },
}

impl HighFreqArbBot {
    pub fn new(config: Config, clob_client: ClobClient) -> Self {
        let market_cache = MarketCache::new(config.target_assets.clone());
        let binance_symbols = config.binance_symbols();
        let binance_feed = BinanceFeed::new(binance_symbols);
        let clob_feed = ClobFeed::new();
        let binance_rest = BinanceRest::new();
        let edge_detector = EdgeDetector::default();

        Self {
            config,
            clob_client,
            market_cache,
            binance_rest,
            edge_detector,
            binance_feed,
            clob_feed,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            positions: HashMap::new(),
            current_window_end: None,
        }
    }

    pub async fn run(&mut self) {
        info!("Starting bot main loop");

        // Initialize CLOB feed with current trading pairs before starting
        if !self.markets.is_empty() {
            let pairs = self.get_trading_pairs();
            self.clob_feed.set_pairs(pairs);
        }

        // Start WebSocket feeds
        self.binance_feed.start();
        self.clob_feed.start();

        let mut last_edge_log = std::time::Instant::now();

        loop {
            let now = Utc::now();

            // Rotate markets if window ended
            if self.should_rotate_markets(now) {
                info!("Market window ended, rotating...");
                self.positions.clear();
                self.discover_markets().await;

                // Update CLOB feed with new trading pairs
                let pairs = self.get_trading_pairs();
                self.clob_feed.set_pairs(pairs);

                if self.markets.is_empty() {
                    warn!("No markets available, waiting 30s...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }

            // Scan for edge opportunities (Binance vs PM divergence)
            let edges = self.scan_for_edges().await;
            
            // Log edges periodically (every 2 seconds if any exist)
            if !edges.is_empty() && last_edge_log.elapsed() > Duration::from_secs(2) {
                for edge in &edges {
                    info!("EDGE: {}", edge);
                }
                last_edge_log = std::time::Instant::now();
            }

            // Execute on strong edges
            for edge in edges {
                if edge.strength == EdgeStrength::Strong || edge.strength == EdgeStrength::Moderate {
                    self.execute_edge_entry(&edge).await;
                }
            }

            // Also scan for combined arb opportunities (existing logic)
            self.scan_for_combined_arbs().await;

            // Short sleep - WebSocket prices update continuously
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Scan for edges: Binance price movement vs PM prices
    pub async fn scan_for_edges(&self) -> Vec<EdgeSignal> {
        let now = Utc::now();
        let mut signals = Vec::new();

        for (market_id, state) in &self.markets {
            // Skip if we already have a position in this market
            if self.positions.contains_key(market_id) {
                continue;
            }

            // Skip if market ended or not started yet
            if now >= state.end_time || now < state.start_time {
                continue;
            }

            // Need open price from Binance
            let open_price = match state.binance_open_price {
                Some(p) => p,
                None => continue,
            };

            // Get current Binance price
            let current_price = match self.binance_feed.get_price(&state.binance_symbol) {
                Some(p) => p.price,
                None => continue,
            };

            // Get PM prices
            let (up_ask, down_ask) = {
                let pair = state.pair.read();
                (pair.up_ask, pair.down_ask)
            };

            let time_elapsed_pct = state.elapsed_pct(now);

            // Run edge detection
            if let Some(signal) = self.edge_detector.analyze(
                &state.info.asset,
                market_id,
                open_price,
                current_price,
                up_ask,
                down_ask,
                time_elapsed_pct,
            ) {
                signals.push(signal);
            }
        }

        signals
    }

    async fn execute_edge_entry(&mut self, edge: &EdgeSignal) {
        let state = match self.markets.get(&edge.market_id) {
            Some(s) => s,
            None => return,
        };

        // Determine which side to buy based on direction
        let (token_id, price, side) = match edge.direction {
            Direction::Up => (&state.info.up_token_id, edge.pm_up_ask, Side::Up),
            Direction::Down => (&state.info.down_token_id, edge.pm_down_ask, Side::Down),
            Direction::Neutral => return,
        };

        // Check price limit
        if price > self.config.arb_config.max_limit_price {
            debug!(
                "{} Edge price {} > max {}, skipping",
                edge.asset, price, self.config.arb_config.max_limit_price
            );
            return;
        }

        let size = self.config.arb_config.shares_per_side;

        info!(
            "Executing edge entry: {} {:?} @ {} (strength={:?}, move={:.3}%)",
            edge.asset,
            side,
            price,
            edge.strength,
            edge.price_change_pct * Decimal::from(100)
        );

        let result = self.clob_client.place_limit_order(token_id, price, size).await;

        if result.placed() {
            info!(
                "{} {:?} order placed: {} @ {}",
                edge.asset,
                side,
                result.order_id.as_deref().unwrap_or("?"),
                price
            );

            let position = Position {
                market_id: edge.market_id.clone(),
                asset: edge.asset.clone(),
                side,
                order_id: result.order_id,
                price,
                size,
                created_at: Utc::now(),
                entry_reason: EntryReason::EdgeSignal {
                    direction: edge.direction,
                    strength: edge.strength,
                },
            };

            self.positions.insert(edge.market_id.clone(), position);
        } else {
            warn!(
                "{} {:?} order FAILED: {}",
                edge.asset,
                side,
                result.error.as_deref().unwrap_or("unknown")
            );
        }
    }

    /// Original combined arb scan (UP + DOWN < threshold)
    pub async fn scan_for_combined_arbs(&mut self) {
        let threshold = self.config.arb_config.arb_threshold;
        let size = self.config.arb_config.shares_per_side;
        let now = Utc::now();
        let now_ms = crate::models::now_ms();

        let mut opportunities: Vec<(String, Decimal, Decimal)> = Vec::new();

        for (market_id, state) in &self.markets {
            if self.positions.contains_key(market_id) {
                continue;
            }

            if now >= state.end_time {
                continue;
            }

            let pair = state.pair.read();

            let (up_ask, down_ask) = match (pair.up_ask, pair.down_ask) {
                (Some(u), Some(d)) => (u, d),
                _ => continue,
            };

            // Skip stale prices
            if pair.last_update_ms > 0 && (now_ms - pair.last_update_ms) > 5000 {
                continue;
            }

            let combined = up_ask + down_ask;

            if combined <= threshold {
                info!(
                    "COMBINED ARB: {} | UP={} DOWN={} combined={} (threshold={})",
                    state.info.asset, up_ask, down_ask, combined, threshold
                );
                opportunities.push((market_id.clone(), up_ask, down_ask));
            }
        }

        for (market_id, up_ask, down_ask) in opportunities {
            self.execute_combined_arb(&market_id, up_ask, down_ask, size).await;
        }
    }

    async fn execute_combined_arb(
        &mut self,
        market_id: &str,
        up_price: Decimal,
        down_price: Decimal,
        size: Decimal,
    ) {
        let state = match self.markets.get(market_id) {
            Some(s) => s,
            None => return,
        };

        let asset = &state.info.asset;
        let up_token = &state.info.up_token_id;
        let down_token = &state.info.down_token_id;

        info!(
            "Executing combined arb: {} | UP@{} DOWN@{} size={}",
            asset, up_price, down_price, size
        );

        let up_result = self.clob_client.place_limit_order(up_token, up_price, size).await;
        let down_result = self.clob_client.place_limit_order(down_token, down_price, size).await;

        if up_result.placed() {
            info!("{} UP order placed: {} @ {}", asset, up_result.order_id.as_deref().unwrap_or("?"), up_price);
        } else {
            warn!("{} UP order FAILED: {}", asset, up_result.error.as_deref().unwrap_or("unknown"));
        }

        if down_result.placed() {
            info!("{} DOWN order placed: {} @ {}", asset, down_result.order_id.as_deref().unwrap_or("?"), down_price);
        } else {
            warn!("{} DOWN order FAILED: {}", asset, down_result.error.as_deref().unwrap_or("unknown"));
        }

        // Cancel orphan if one leg failed
        if up_result.not_placed() != down_result.not_placed() {
            let to_cancel = if up_result.placed() { &up_result } else { &down_result };
            if let Some(order_id) = &to_cancel.order_id {
                warn!("{} Cancelling orphan order {} (other leg failed)", asset, order_id);
                if let Err(e) = self.clob_client.cancel_order(order_id).await {
                    error!("Failed to cancel orphan order: {}", e);
                }
            }
            return;
        }

        // Both succeeded - we don't track combined arb positions the same way
        // since we have both sides hedged
        if up_result.placed() && down_result.placed() {
            let combined_cost = up_price + down_price;
            let profit = Decimal::ONE - combined_cost;
            info!("{} Combined position opened | cost={} profit_per_share={}", asset, combined_cost, profit);

            // Track as UP position (arbitrary, just to prevent re-entry)
            let position = Position {
                market_id: market_id.to_string(),
                asset: asset.clone(),
                side: Side::Up,
                order_id: up_result.order_id,
                price: up_price,
                size,
                created_at: Utc::now(),
                entry_reason: EntryReason::CombinedArb { combined_cost },
            };
            self.positions.insert(market_id.to_string(), position);
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

        // Filter to markets that haven't ended yet
        let mut valid: Vec<MarketInfo> = all_markets
            .into_iter()
            .filter(|m| m.end_time > now)
            .collect();

        valid.sort_by_key(|m| m.end_time);

        // Select one market per asset (earliest ending)
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

        // Build MarketState for each selected market and fetch open prices
        for (asset, info) in &selected {
            let binance_symbol = ASSETS_BY_NAME
                .get(asset)
                .map(|a| a.binance.clone())
                .unwrap_or_default();

            let pair = Arc::new(RwLock::new(info.to_trading_pair()));

            // Fetch the candle open price from Binance
            let binance_open_price = self.fetch_open_price(&binance_symbol, info.start_time).await;

            if let Some(open) = binance_open_price {
                info!("{} market open price: {} (from {})", asset, open, info.start_time);
            } else {
                warn!("{} could not fetch open price from Binance", asset);
            }

            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol,
                start_time: info.start_time,
                end_time: info.end_time,
                binance_open_price,
            };

            self.markets.insert(info.id.clone(), state);
            self.trading_pairs.insert(info.id.clone(), pair);
        }

        self.current_window_end = self.markets.values().map(|s| s.end_time).min();
        self.log_discovered_markets();
    }

    async fn fetch_open_price(&self, symbol: &str, start_time: DateTime<Utc>) -> Option<Decimal> {
        // Try to get the 15m candle that starts at market start time
        match self.binance_rest.get_kline_at(symbol, "15m", start_time).await {
            Ok(Some(kline)) => Some(kline.open),
            Ok(None) => {
                // Fallback: get current price as approximation if market just started
                match self.binance_rest.get_price(symbol).await {
                    Ok(p) => Some(p),
                    Err(e) => {
                        warn!("Failed to get fallback price for {}: {}", symbol, e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("Failed to fetch kline for {}: {}", symbol, e);
                // Fallback to current price
                self.binance_rest.get_price(symbol).await.ok()
            }
        }
    }

    fn log_discovered_markets(&self) {
        if self.markets.is_empty() {
            return;
        }

        let assets: Vec<&str> = self
            .markets
            .values()
            .map(|s| s.info.asset.as_str())
            .collect();

        if let Some(first) = self.markets.values().next() {
            let start_est = first.start_time.with_timezone(&New_York);
            let end_est = first.end_time.with_timezone(&New_York);

            info!(
                "Discovered {} markets: {:?} | {}-{} EST",
                self.markets.len(),
                assets,
                start_est.format("%H:%M"),
                end_est.format("%H:%M")
            );
        }
    }

    pub fn should_rotate_markets(&self, now: DateTime<Utc>) -> bool {
        match self.current_window_end {
            Some(end) => now >= end,
            None => true,
        }
    }

    pub fn get_trading_pairs(&self) -> Vec<Arc<RwLock<TradingPair>>> {
        self.trading_pairs.values().cloned().collect()
    }

    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }

    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn test_config() -> Config {
        Config {
            dry_run: true,
            polymarket_private_key: String::new(),
            polymarket_proxy_address: String::new(),
            target_assets: HashSet::from(["bitcoin".to_string(), "solana".to_string()]),
            arb_config: Default::default(),
        }
    }
}
