use chrono::{DateTime, TimeDelta, Utc};
use parking_lot::RwLock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "Up")]
    Up,
    #[serde(rename = "Down")]
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Opening,
    Midgame,
    Endgame,
    Closed,
}

/// All identifiers for a single binary market.
/// Polymarket uses different IDs in different API contexts.
#[derive(Debug, Clone)]
pub struct MarketIds {
    /// Numeric ID from Gamma API (e.g., "12345").
    /// Used for: internal tracking, some REST endpoints.
    pub gamma_id: String,

    /// Hex condition ID (e.g., "0xabc123...").
    /// Used for: WebSocket subscriptions, CLOB market identification.
    pub condition_id: String,

    /// CLOB token ID for the "Up" outcome.
    pub up_token: String,

    /// CLOB token ID for the "Down" outcome.
    pub down_token: String,
}

impl MarketIds {
    pub fn token_for_side(&self, side: Side) -> &str {
        match side {
            Side::Up => &self.up_token,
            Side::Down => &self.down_token,
        }
    }

    pub fn side_for_token(&self, token_id: &str) -> Option<Side> {
        if token_id == self.up_token {
            Some(Side::Up)
        } else if token_id == self.down_token {
            Some(Side::Down)
        } else {
            None
        }
    }

    pub fn both_tokens(&self) -> [&str; 2] {
        [&self.up_token, &self.down_token]
    }
}

#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub ids: MarketIds,
    pub slug: String,
    pub asset: String,
    pub binance_symbol: String,
    pub duration: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

impl MarketInfo {
    pub fn to_trading_pair(&self) -> TradingPair {
        TradingPair {
            asset: self.asset.clone(),
            duration: self.duration.clone(),
            gamma_id: self.ids.gamma_id.clone(),
            up_token_id: self.ids.up_token.clone(),
            down_token_id: self.ids.down_token.clone(),
            end_time: self.end_time,
            rest_up_bid: None,
            rest_up_ask: None,
            rest_down_bid: None,
            rest_down_ask: None,
            last_rest_update_ms: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradingPair {
    pub asset: String,
    pub duration: String,
    pub gamma_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub end_time: DateTime<Utc>,

    pub rest_up_bid: Option<Decimal>,
    pub rest_up_ask: Option<Decimal>,
    pub rest_down_bid: Option<Decimal>,
    pub rest_down_ask: Option<Decimal>,
    pub last_rest_update_ms: i64,
}

impl TradingPair {
    pub fn latest_up_ask(&self) -> Option<Decimal> {
        self.rest_up_ask
    }

    pub fn latest_down_ask(&self) -> Option<Decimal> {
        self.rest_down_ask
    }

    pub fn combined_ask(&self) -> Option<Decimal> {
        Some(self.latest_up_ask()? + self.latest_down_ask()?)
    }

    pub fn has_both_asks(&self) -> bool {
        self.latest_up_ask().is_some() && self.latest_down_ask().is_some()
    }

    pub fn clear_prices(&mut self) {
        self.rest_up_bid = None;
        self.rest_up_ask = None;
        self.rest_down_bid = None;
        self.rest_down_ask = None;
        self.last_rest_update_ms = 0;
    }
}

impl std::fmt::Display for TradingPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]: {} REST_UP: {:?}",
            self.asset.to_uppercase(),
            self.duration,
            self.rest_up_ask
        )
    }
}

#[derive(Debug)]
pub struct MarketState {
    pub pair: Arc<RwLock<TradingPair>>,
    pub info: MarketInfo,
    pub binance_symbol: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub binance_open_price: Option<Decimal>,
}

impl MarketState {
    pub fn elapsed_seconds(&self, now: DateTime<Utc>) -> f64 {
        (now - self.start_time).num_milliseconds() as f64 / 1000.0
    }

    pub fn remaining_seconds(&self, now: DateTime<Utc>) -> f64 {
        (self.end_time - now).num_milliseconds() as f64 / 1000.0
    }

    pub fn elapsed_pct(&self, now: DateTime<Utc>) -> f64 {
        let total = (self.end_time - self.start_time).num_milliseconds() as f64;
        if total <= 0.0 {
            return 1.0;
        }
        let elapsed = (now - self.start_time).num_milliseconds() as f64;
        (elapsed / total).min(1.0).max(0.0)
    }

    pub fn phase(&self, now: DateTime<Utc>) -> Phase {
        let remaining = self.remaining_seconds(now);
        let elapsed = self.elapsed_seconds(now);

        if remaining <= 0.0 {
            Phase::Closed
        } else if elapsed < 60.0 {
            Phase::Opening
        } else if remaining <= 120.0 {
            Phase::Endgame
        } else {
            Phase::Midgame
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketQuotes {
    pub up_orders: Vec<RestingOrder>,
    pub down_orders: Vec<RestingOrder>,
}

impl MarketQuotes {
    pub fn side_orders(&self, side: Side) -> &Vec<RestingOrder> {
        match side {
            Side::Up => &self.up_orders,
            Side::Down => &self.down_orders,
        }
    }

    pub fn side_orders_mut(&mut self, side: Side) -> &mut Vec<RestingOrder> {
        match side {
            Side::Up => &mut self.up_orders,
            Side::Down => &mut self.down_orders,
        }
    }

    pub fn order_for_side(&self, side: Side) -> Option<&RestingOrder> {
        self.side_orders(side)
            .iter()
            .max_by(|a, b| a.price.cmp(&b.price))
    }

    pub fn find_order_by_id(&self, order_id: &str) -> Option<(&RestingOrder, Side)> {
        for o in &self.up_orders {
            if o.order_id == order_id {
                return Some((o, Side::Up));
            }
        }
        for o in &self.down_orders {
            if o.order_id == order_id {
                return Some((o, Side::Down));
            }
        }
        None
    }

    pub fn find_order_mut_by_id(&mut self, order_id: &str) -> Option<(&mut RestingOrder, Side)> {
        for o in &mut self.up_orders {
            if o.order_id == order_id {
                return Some((o, Side::Up));
            }
        }
        for o in &mut self.down_orders {
            if o.order_id == order_id {
                return Some((o, Side::Down));
            }
        }
        None
    }

    pub fn clear_order_by_id(&mut self, order_id: &str) {
        self.up_orders.retain(|o| o.order_id != order_id);
        self.down_orders.retain(|o| o.order_id != order_id);
    }
}

#[derive(Debug, Clone)]
pub struct OrderEvent {
    pub order_id: String,
    pub gamma_id: String,
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size_matched: Decimal,
    pub msg_type: String,
}

/// Unified fill event - the single source of truth for inventory updates.
/// Generated from WebSocket Trade messages for both maker and taker fills.
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub trade_id: String,
    pub gamma_id: String,
    pub token_id: String,
    pub order_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub is_taker: bool,
}

#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order_id: String,
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub filled: Decimal,
    pub posted_at: Instant,
    pub second_leg: Option<SecondLegParams>,
}

#[derive(Debug, Clone)]
pub struct SecondLegParams {
    pub token_id: String,
    pub side: Side,
    pub max_price: Decimal,
}

impl RestingOrder {
    pub fn remaining(&self) -> Decimal {
        (self.size - self.filled).max(Decimal::ZERO)
    }

    pub fn is_fully_filled(&self) -> bool {
        self.filled >= self.size
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketInventory {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub up_cost_basis: Decimal,
    pub down_cost_basis: Decimal,
}

impl MarketInventory {
    pub fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }

    pub fn total_pairs(&self) -> Decimal {
        self.up_shares.min(self.down_shares)
    }

    pub fn avg_price(&self, side: Side) -> Option<Decimal> {
        let (shares, cost) = match side {
            Side::Up => (self.up_shares, self.up_cost_basis),
            Side::Down => (self.down_shares, self.down_cost_basis),
        };
        if shares > Decimal::ZERO {
            Some(cost / shares)
        } else {
            None
        }
    }

    pub fn add_fill(&mut self, side: Side, size: Decimal, price: Decimal) {
        match side {
            Side::Up => {
                self.up_shares += size;
                self.up_cost_basis += price * size;
            }
            Side::Down => {
                self.down_shares += size;
                self.down_cost_basis += price * size;
            }
        }
    }
}

#[derive(Clone)]
pub struct MarketLookup {
    condition_to_gamma: HashMap<String, String>,
    gamma_to_ids: HashMap<String, MarketIds>,
    token_to_gamma: HashMap<String, String>,
}

impl MarketLookup {
    pub fn new(markets: &HashMap<String, MarketState>) -> Self {
        let mut condition_to_gamma = HashMap::new();
        let mut gamma_to_ids = HashMap::new();
        let mut token_to_gamma = HashMap::new();

        for (gamma_id, state) in markets {
            let ids = &state.info.ids;
            condition_to_gamma.insert(ids.condition_id.clone(), gamma_id.clone());
            gamma_to_ids.insert(gamma_id.clone(), ids.clone());
            token_to_gamma.insert(ids.up_token.clone(), gamma_id.clone());
            token_to_gamma.insert(ids.down_token.clone(), gamma_id.clone());
        }

        Self {
            condition_to_gamma,
            gamma_to_ids,
            token_to_gamma,
        }
    }

    pub fn resolve_condition(&self, condition_id: &str) -> Option<&str> {
        self.condition_to_gamma
            .get(condition_id)
            .map(|s| s.as_str())
    }

    pub fn resolve_token(&self, token_id: &str) -> Option<&str> {
        self.token_to_gamma.get(token_id).map(|s| s.as_str())
    }

    pub fn get_ids(&self, gamma_id: &str) -> Option<&MarketIds> {
        self.gamma_to_ids.get(gamma_id)
    }

    pub fn all_tokens(&self) -> Vec<String> {
        self.gamma_to_ids
            .values()
            .flat_map(|ids| vec![ids.up_token.clone(), ids.down_token.clone()])
            .collect()
    }

    pub fn all_condition_ids(&self) -> Vec<String> {
        self.condition_to_gamma.keys().cloned().collect()
    }
}

pub fn duration_label(delta: TimeDelta) -> String {
    let minutes = delta.num_minutes();
    match minutes {
        15 => "15m".into(),
        60 => "1hr".into(),
        240 => "4hr".into(),
        _ => format!("{}m", minutes),
    }
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
