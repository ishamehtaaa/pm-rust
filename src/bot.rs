use crate::clob_client::{ClobClient, PriceSide};
use crate::config::{Config, ASSETS_BY_NAME};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};
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
    pub up_order_id: Option<String>,
    pub down_order_id: Option<String>,
    pub up_price: Decimal,
    pub down_price: Decimal,
    pub size: Decimal,
    pub created_at: DateTime<Utc>,
}

impl HighFreqArbBot {
    pub fn new(config: Config, clob_client: ClobClient) -> Self {
        let market_cache = MarketCache::new(config.target_assets.clone());

        Self {
            config,
            clob_client,
            market_cache,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            positions: HashMap::new(),
            current_window_end: None,
        }
    }

    pub async fn run(&mut self) {
        info!("Starting bot main loop");

        loop {
            let now = Utc::now();

            // Rotate markets if window ended
            if self.should_rotate_markets(now) {
                info!("Market window ended, rotating...");
                self.positions.clear();
                self.discover_markets().await;

                if self.markets.is_empty() {
                    warn!("No markets available, waiting 30s...");
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }

            // Scan for arb opportunities
            if let Err(e) = self.scan_for_arbs().await {
                error!("Arb scan error: {}", e);
            }

            // Small delay to avoid hammering the API
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn scan_for_arbs(&mut self) -> anyhow::Result<()> {
        let threshold = self.config.arb_config.arb_threshold;
        let size = self.config.arb_config.shares_per_side;
        let now = Utc::now();

        // Collect opportunities first to avoid borrow issues
        let mut opportunities: Vec<(String, Decimal, Decimal)> = Vec::new();

        for (market_id, state) in &self.markets {
            // Skip if we already have a position
            if self.positions.contains_key(market_id) {
                continue;
            }

            // Skip if market ended
            if now >= state.end_time {
                continue;
            }

            // Fetch current best asks
            let up_ask = match self
                .clob_client
                .get_best_price(&state.info.up_token_id, PriceSide::Buy)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    debug!("Failed to get UP ask for {}: {}", state.info.asset, e);
                    continue;
                }
            };

            let down_ask = match self
                .clob_client
                .get_best_price(&state.info.down_token_id, PriceSide::Buy)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    debug!("Failed to get DOWN ask for {}: {}", state.info.asset, e);
                    continue;
                }
            };

            let combined = up_ask + down_ask;

            debug!(
                "{} | UP={} DOWN={} combined={}",
                state.info.asset, up_ask, down_ask, combined
            );

            // Check if arb threshold is met
            if combined <= threshold {
                info!(
                    "ARB TRIGGERED: {} | combined={} (threshold={})",
                    state.info.asset, combined, threshold
                );
                opportunities.push((market_id.clone(), up_ask, down_ask));
            }
        }

        // Execute entries outside the borrow
        for (market_id, up_ask, down_ask) in opportunities {
            self.execute_arb_entry(&market_id, up_ask, down_ask, size).await;
        }

        Ok(())
    }

    async fn execute_arb_entry(
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
            "Executing arb entry: {} | UP@{} DOWN@{} size={}",
            asset, up_price, down_price, size
        );

        // Place both orders
        let up_result = self
            .clob_client
            .place_limit_order(up_token, up_price, size)
            .await;

        let down_result = self
            .clob_client
            .place_limit_order(down_token, down_price, size)
            .await;

        // Log results
        if up_result.placed() {
            info!(
                "{} UP order placed: {} @ {}",
                asset,
                up_result.order_id.as_deref().unwrap_or("?"),
                up_price
            );
        } else {
            warn!(
                "{} UP order FAILED: {}",
                asset,
                up_result.error.as_deref().unwrap_or("unknown")
            );
        }

        if down_result.placed() {
            info!(
                "{} DOWN order placed: {} @ {}",
                asset,
                down_result.order_id.as_deref().unwrap_or("?"),
                down_price
            );
        } else {
            warn!(
                "{} DOWN order FAILED: {}",
                asset,
                down_result.error.as_deref().unwrap_or("unknown")
            );
        }

        // If one leg failed, cancel the other to avoid unhedged exposure
        if up_result.not_placed() != down_result.not_placed() {
            let to_cancel = if up_result.placed() {
                &up_result
            } else {
                &down_result
            };

            if let Some(order_id) = &to_cancel.order_id {
                warn!(
                    "{} Cancelling orphan order {} (other leg failed)",
                    asset, order_id
                );
                if let Err(e) = self.clob_client.cancel_order(order_id).await {
                    error!("Failed to cancel orphan order: {}", e);
                }
            }
            return;
        }

        // Both succeeded - track position
        if up_result.placed() && down_result.placed() {
            let position = Position {
                market_id: market_id.to_string(),
                asset: asset.clone(),
                up_order_id: up_result.order_id,
                down_order_id: down_result.order_id,
                up_price,
                down_price,
                size,
                created_at: Utc::now(),
            };

            let profit = Decimal::ONE - (up_price + down_price);
            info!(
                "{} Position opened | cost={} profit_per_share={}",
                asset,
                up_price + down_price,
                profit
            );

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

        // Sort by end_time (earliest first)
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

        // Clear old state
        self.markets.clear();
        self.trading_pairs.clear();

        // Build MarketState for each selected market
        for (asset, info) in &selected {
            let binance_symbol = ASSETS_BY_NAME
                .get(asset)
                .map(|a| a.binance.clone())
                .unwrap_or_default();

            let pair = Arc::new(RwLock::new(info.to_trading_pair()));

            let state = MarketState {
                pair: pair.clone(),
                info: info.clone(),
                binance_symbol,
                start_time: info.start_time,
                end_time: info.end_time,
                binance_open_price: None,
            };

            self.markets.insert(info.id.clone(), state);
            self.trading_pairs.insert(info.id.clone(), pair);
        }

        // Set window end to earliest market end
        self.current_window_end = self.markets.values().map(|s| s.end_time).min();

        self.log_discovered_markets();
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
