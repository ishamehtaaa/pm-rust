// arb_finder/market_state.rs
//
// Aggregated view of a market's current state, combining data from
// multiple sources (WS orderbook, REST snapshots, trades).

use parking_lot::RwLock;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A single trade event
#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub timestamp: Instant,
    pub token_id: String,
    pub side: TradeSide,
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Price level in the order book
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Order book depth snapshot
#[derive(Debug, Clone, Default)]
pub struct DepthSnapshot {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: Option<Instant>,
}

impl DepthSnapshot {
    /// Total bid liquidity
    pub fn total_bid_liquidity(&self) -> Decimal {
        self.bids.iter().map(|l| l.size).sum()
    }

    /// Total ask liquidity
    pub fn total_ask_liquidity(&self) -> Decimal {
        self.asks.iter().map(|l| l.size).sum()
    }

    /// Best bid price
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|l| l.price)
    }

    /// Best ask price
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.price)
    }

    /// Calculate how much price would move if a given size were sold
    pub fn price_impact_for_sell(&self, size: Decimal) -> Option<Decimal> {
        if self.bids.is_empty() {
            return None;
        }

        let mut remaining = size;
        let mut last_price = self.bids[0].price;

        for level in &self.bids {
            if remaining <= Decimal::ZERO {
                break;
            }
            last_price = level.price;
            remaining -= level.size;
        }

        Some(self.bids[0].price - last_price)
    }
}

/// Aggregated state for a single token (Up or Down)
#[derive(Debug, Clone)]
pub struct TokenState {
    /// WebSocket best bid
    pub ws_bid: Option<Decimal>,
    /// WebSocket best ask
    pub ws_ask: Option<Decimal>,
    /// REST API best ask (may differ from WS)
    pub rest_ask: Option<Decimal>,
    /// Last WS update time
    pub ws_updated: Option<Instant>,
    /// Last REST update time
    pub rest_updated: Option<Instant>,
    /// Full order book depth from REST
    pub rest_depth: DepthSnapshot,
    /// Full order book depth from WebSocket
    pub ws_depth: DepthSnapshot,
    /// Recent trades (sliding window)
    pub recent_trades: VecDeque<TradeEvent>,
    /// Price history for velocity calculation
    pub price_history: VecDeque<(Instant, Decimal)>,
}

impl Default for TokenState {
    fn default() -> Self {
        Self {
            ws_bid: None,
            ws_ask: None,
            rest_ask: None,
            ws_updated: None,
            rest_updated: None,
            rest_depth: DepthSnapshot::default(),
            ws_depth: DepthSnapshot::default(),
            recent_trades: VecDeque::with_capacity(100),
            price_history: VecDeque::with_capacity(100),
        }
    }
}

impl TokenState {
    /// Get the best available ask price (prefer REST if fresher)
    pub fn best_ask(&self) -> Option<Decimal> {
        // If REST is more recent and different, it might be capturing a sweep
        match (self.rest_ask, self.rest_updated, self.ws_ask, self.ws_updated) {
            (Some(rest), Some(rest_t), Some(ws), Some(ws_t)) => {
                if rest_t > ws_t {
                    Some(rest)
                } else {
                    Some(ws)
                }
            }
            (Some(rest), _, None, _) => Some(rest),
            (None, _, Some(ws), _) => Some(ws),
            _ => None,
        }
    }

    /// Get all ask levels from the best available source (REST preferred if fresh)
    /// Returns sorted by price ascending (cheapest first)
    pub fn all_asks(&self) -> Vec<PriceLevel> {
        let now = Instant::now();
        let rest_age = self.rest_updated.map(|t| now.duration_since(t));
        let ws_age = self.ws_updated.map(|t| now.duration_since(t));

        // Use REST depth if it's fresh (< 500ms) or if WS depth is empty/stale
        let use_rest = match (rest_age, ws_age) {
            (Some(r), Some(w)) if r < std::time::Duration::from_millis(500) => true,
            (Some(_), None) => true,
            (Some(r), Some(w)) if r < w => true,
            _ => false,
        };

        if use_rest && !self.rest_depth.asks.is_empty() {
            self.rest_depth.asks.clone()
        } else if !self.ws_depth.asks.is_empty() {
            self.ws_depth.asks.clone()
        } else {
            // Fallback to constructing from best ask
            self.best_ask()
                .map(|p| vec![PriceLevel { price: p, size: Decimal::from(100) }])
                .unwrap_or_default()
        }
    }

    /// Get the minimum ask price across all depth levels
    pub fn min_ask(&self) -> Option<Decimal> {
        let all = self.all_asks();
        all.iter().map(|l| l.price).min()
    }

    /// Update from WebSocket orderbook
    pub fn update_ws(&mut self, bid: Decimal, ask: Decimal) {
        let now = Instant::now();
        self.ws_bid = Some(bid);
        self.ws_ask = Some(ask);
        self.ws_updated = Some(now);

        // Record price history
        self.price_history.push_back((now, ask));
        if self.price_history.len() > 100 {
            self.price_history.pop_front();
        }
    }

    /// Update from WebSocket with full depth
    pub fn update_ws_depth(&mut self, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        let now = Instant::now();
        
        // Update best bid/ask from depth
        if let Some(best_bid) = bids.iter().map(|l| l.price).max() {
            self.ws_bid = Some(best_bid);
        }
        if let Some(best_ask) = asks.iter().map(|l| l.price).min() {
            self.ws_ask = Some(best_ask);
            // Record price history
            self.price_history.push_back((now, best_ask));
            if self.price_history.len() > 100 {
                self.price_history.pop_front();
            }
        }
        
        self.ws_depth = DepthSnapshot {
            bids,
            asks,
            timestamp: Some(now),
        };
        self.ws_updated = Some(now);
    }

    /// Update from REST snapshot
    pub fn update_rest(&mut self, ask: Decimal) {
        self.rest_ask = Some(ask);
        self.rest_updated = Some(Instant::now());
    }

    /// Update from REST with full depth
    pub fn update_rest_depth(&mut self, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        let now = Instant::now();
        
        // Update best ask from depth
        if let Some(best_ask) = asks.iter().map(|l| l.price).min() {
            self.rest_ask = Some(best_ask);
        }
        
        self.rest_depth = DepthSnapshot {
            bids,
            asks,
            timestamp: Some(now),
        };
        self.rest_updated = Some(now);
    }

    /// Record a trade
    pub fn record_trade(&mut self, trade: TradeEvent) {
        self.recent_trades.push_back(trade);
        if self.recent_trades.len() > 100 {
            self.recent_trades.pop_front();
        }
    }

    /// Calculate price velocity (change per second) over a window
    pub fn price_velocity(&self, window: Duration) -> Option<Decimal> {
        let now = Instant::now();
        let cutoff = now - window;

        let recent: Vec<_> = self
            .price_history
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .collect();

        if recent.len() < 2 {
            return None;
        }

        let first = recent.first()?;
        let last = recent.last()?;

        let price_change = last.1 - first.1;
        let time_secs = last.0.duration_since(first.0).as_secs_f64();

        if time_secs < 0.1 {
            return None;
        }

        Some(price_change / Decimal::try_from(time_secs).ok()?)
    }

    /// Get recent trade volume in a window
    pub fn recent_trade_volume(&self, window: Duration) -> Decimal {
        let cutoff = Instant::now() - window;
        self.recent_trades
            .iter()
            .filter(|t| t.timestamp >= cutoff)
            .map(|t| t.size)
            .sum()
    }

    /// Get largest recent trade
    pub fn largest_recent_trade(&self, window: Duration) -> Option<&TradeEvent> {
        let cutoff = Instant::now() - window;
        self.recent_trades
            .iter()
            .filter(|t| t.timestamp >= cutoff)
            .max_by(|a, b| a.size.partial_cmp(&b.size).unwrap())
    }

    /// REST/WS price discrepancy
    pub fn rest_ws_discrepancy(&self) -> Option<Decimal> {
        match (self.rest_ask, self.ws_ask) {
            (Some(rest), Some(ws)) => Some((rest - ws).abs()),
            _ => None,
        }
    }
}

/// Complete market state (Up + Down tokens)
#[derive(Debug, Default, Clone)]
pub struct MarketState {
    pub market_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub up: TokenState,
    pub down: TokenState,
}

/// An arbitrage opportunity at specific price levels
#[derive(Debug, Clone)]
pub struct ArbOpportunity {
    pub up_price: Decimal,
    pub up_size: Decimal,
    pub down_price: Decimal,
    pub down_size: Decimal,
    pub combined: Decimal,
    pub max_size: Decimal,
    pub profit_per_share: Decimal,
}

impl MarketState {
    pub fn new(market_id: String, up_token_id: String, down_token_id: String) -> Self {
        Self {
            market_id,
            up_token_id,
            down_token_id,
            up: TokenState::default(),
            down: TokenState::default(),
        }
    }

    /// Combined ask price (what we'd pay to buy both sides) - uses best ask only
    pub fn combined_ask(&self) -> Option<Decimal> {
        Some(self.up.best_ask()? + self.down.best_ask()?)
    }

    /// Combined ask from WS only
    pub fn combined_ws_ask(&self) -> Option<Decimal> {
        Some(self.up.ws_ask? + self.down.ws_ask?)
    }

    /// Combined ask from REST only
    pub fn combined_rest_ask(&self) -> Option<Decimal> {
        Some(self.up.rest_ask? + self.down.rest_ask?)
    }

    /// Find ALL arbitrage opportunities by scanning cross-product of up asks x down asks
    /// Returns opportunities sorted by profit (best first)
    pub fn find_arb_opportunities(&self, threshold: Decimal) -> Vec<ArbOpportunity> {
        let up_asks = self.up.all_asks();
        let down_asks = self.down.all_asks();
        
        let mut opportunities = Vec::new();
        
        for up_level in &up_asks {
            for down_level in &down_asks {
                let combined = up_level.price + down_level.price;
                if combined < threshold {
                    let max_size = up_level.size.min(down_level.size);
                    let profit_per_share = Decimal::ONE - combined;
                    
                    opportunities.push(ArbOpportunity {
                        up_price: up_level.price,
                        up_size: up_level.size,
                        down_price: down_level.price,
                        down_size: down_level.size,
                        combined,
                        max_size,
                        profit_per_share,
                    });
                }
            }
        }
        
        // Sort by profit per share descending (best opportunities first)
        opportunities.sort_by(|a, b| b.profit_per_share.partial_cmp(&a.profit_per_share).unwrap());
        
        opportunities
    }

    /// Find the single best arb opportunity if one exists
    pub fn best_arb_opportunity(&self, threshold: Decimal) -> Option<ArbOpportunity> {
        self.find_arb_opportunities(threshold).into_iter().next()
    }

    /// Get minimum combined price across all depth levels
    pub fn min_combined(&self) -> Option<Decimal> {
        let up_min = self.up.min_ask()?;
        let down_min = self.down.min_ask()?;
        Some(up_min + down_min)
    }

    /// Depth imbalance ratio (up liquidity / down liquidity)
    pub fn depth_imbalance_ratio(&self) -> Option<Decimal> {
        let up_liq = self.up.rest_depth.total_ask_liquidity();
        let down_liq = self.down.rest_depth.total_ask_liquidity();

        if down_liq == Decimal::ZERO {
            return None;
        }

        Some(up_liq / down_liq)
    }

    /// Which side has less liquidity?
    pub fn thin_side(&self) -> Option<ThinSide> {
        let ratio = self.depth_imbalance_ratio()?;

        if ratio > Decimal::ONE {
            Some(ThinSide::Down)
        } else if ratio < Decimal::ONE {
            Some(ThinSide::Up)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThinSide {
    Up,
    Down,
}

/// Thread-safe market state store
#[derive(Clone, Default)]
pub struct MarketStateStore {
    inner: Arc<RwLock<HashMap<String, MarketState>>>,
}

impl MarketStateStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize state for a market
    pub fn init_market(&self, market_id: String, up_token_id: String, down_token_id: String) {
        let mut store = self.inner.write();
        store.insert(
            market_id.clone(),
            MarketState::new(market_id, up_token_id, down_token_id),
        );
    }

    /// Update WS prices for a token
    pub fn update_ws_price(&self, token_id: &str, bid: Decimal, ask: Decimal) {
        let mut store = self.inner.write();
        for state in store.values_mut() {
            if state.up_token_id == token_id {
                state.up.update_ws(bid, ask);
            } else if state.down_token_id == token_id {
                state.down.update_ws(bid, ask);
            }
        }
    }

    /// Update REST price for a token
    pub fn update_rest_price(&self, token_id: &str, ask: Decimal) {
        let mut store = self.inner.write();
        for state in store.values_mut() {
            if state.up_token_id == token_id {
                state.up.update_rest(ask);
            } else if state.down_token_id == token_id {
                state.down.update_rest(ask);
            }
        }
    }

    /// Update REST depth for a token (full order book)
    pub fn update_rest_depth(&self, token_id: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        let mut store = self.inner.write();
        for state in store.values_mut() {
            if state.up_token_id == token_id {
                state.up.update_rest_depth(bids.clone(), asks.clone());
            } else if state.down_token_id == token_id {
                state.down.update_rest_depth(bids.clone(), asks.clone());
            }
        }
    }

    /// Update WS depth for a token (full order book)
    pub fn update_ws_depth(&self, token_id: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        let mut store = self.inner.write();
        for state in store.values_mut() {
            if state.up_token_id == token_id {
                state.up.update_ws_depth(bids.clone(), asks.clone());
            } else if state.down_token_id == token_id {
                state.down.update_ws_depth(bids.clone(), asks.clone());
            }
        }
    }

    /// Record a trade for a token
    pub fn record_trade(&self, token_id: &str, trade: TradeEvent) {
        let mut store = self.inner.write();
        for state in store.values_mut() {
            if state.up_token_id == token_id {
                state.up.record_trade(trade.clone());
            } else if state.down_token_id == token_id {
                state.down.record_trade(trade.clone());
            }
        }
    }

    /// Get a snapshot of market state
    pub fn get_state(&self, market_id: &str) -> Option<MarketState> {
        self.inner.read().get(market_id).cloned()
    }

    /// Get all market states
    pub fn all_states(&self) -> Vec<MarketState> {
        self.inner.read().values().cloned().collect()
    }
}

