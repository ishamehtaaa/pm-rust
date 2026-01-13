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
    /// REST API best bid
    pub rest_bid: Option<Decimal>,
    /// REST API best ask (may differ from WS)
    pub rest_ask: Option<Decimal>,
    /// Last WS update time
    pub ws_updated: Option<Instant>,
    /// Last REST update time
    pub rest_updated: Option<Instant>,
    /// Order book depth
    pub depth: DepthSnapshot,
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
            rest_bid: None,
            rest_ask: None,
            ws_updated: None,
            rest_updated: None,
            depth: DepthSnapshot::default(),
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

    /// Like `best_ask`, but also returns the timestamp of the chosen source.
    pub fn best_ask_with_timestamp(&self) -> Option<(Decimal, Instant)> {
        match (self.rest_ask, self.rest_updated, self.ws_ask, self.ws_updated) {
            (Some(rest), Some(rest_t), Some(ws), Some(ws_t)) => {
                if rest_t > ws_t {
                    Some((rest, rest_t))
                } else {
                    Some((ws, ws_t))
                }
            }
            (Some(rest), Some(rest_t), None, _) => Some((rest, rest_t)),
            (None, _, Some(ws), Some(ws_t)) => Some((ws, ws_t)),
            _ => None,
        }
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

    /// Update from REST snapshot
    pub fn update_rest(&mut self, bid: Decimal, ask: Decimal) {
        self.rest_bid = Some(bid);
        self.rest_ask = Some(ask);
        self.rest_updated = Some(Instant::now());
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

    /// Combined ask price (what we'd pay to buy both sides)
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

    /// Depth imbalance ratio (up liquidity / down liquidity)
    pub fn depth_imbalance_ratio(&self) -> Option<Decimal> {
        let up_liq = self.up.depth.total_ask_liquidity();
        let down_liq = self.down.depth.total_ask_liquidity();

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
    /// token_id -> (market_id, is_up)
    token_index: Arc<RwLock<HashMap<String, (String, bool)>>>,
}

impl MarketStateStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            token_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize state for a market
    pub fn init_market(&self, market_id: String, up_token_id: String, down_token_id: String) {
        {
            let mut store = self.inner.write();
            store.insert(
                market_id.clone(),
                MarketState::new(market_id.clone(), up_token_id.clone(), down_token_id.clone()),
            );
        }

        // Maintain O(1) token_id -> market lookup for hot-path updates
        let mut idx = self.token_index.write();
        idx.insert(up_token_id, (market_id.clone(), true));
        idx.insert(down_token_id, (market_id, false));
    }

    /// Update WS prices for a token
    pub fn update_ws_price(&self, token_id: &str, bid: Decimal, ask: Decimal) {
        let (market_id, is_up) = match self.token_index.read().get(token_id) {
            Some(v) => v.clone(),
            None => return,
        };

        let mut store = self.inner.write();
        let state = match store.get_mut(&market_id) {
            Some(s) => s,
            None => return,
        };

        if is_up {
            state.up.update_ws(bid, ask);
        } else {
            state.down.update_ws(bid, ask);
        }
    }

    /// Update REST price for a token
    pub fn update_rest_price(&self, token_id: &str, bid: Decimal, ask: Decimal) {
        let (market_id, is_up) = match self.token_index.read().get(token_id) {
            Some(v) => v.clone(),
            None => return,
        };

        let mut store = self.inner.write();
        let state = match store.get_mut(&market_id) {
            Some(s) => s,
            None => return,
        };

        if is_up {
            state.up.update_rest(bid, ask);
        } else {
            state.down.update_rest(bid, ask);
        }
    }

    /// Record a trade for a token
    pub fn record_trade(&self, token_id: &str, trade: TradeEvent) {
        let (market_id, is_up) = match self.token_index.read().get(token_id) {
            Some(v) => v.clone(),
            None => return,
        };

        let mut store = self.inner.write();
        let state = match store.get_mut(&market_id) {
            Some(s) => s,
            None => return,
        };

        if is_up {
            state.up.record_trade(trade);
        } else {
            state.down.record_trade(trade);
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

