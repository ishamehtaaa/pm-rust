use chrono::{DateTime, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "Up")]
    Up,
    #[serde(rename = "Down")]
    Down,
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

impl MarketInfo {}

#[derive(Debug)]
pub struct MarketState {
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
}

#[derive(Debug, Clone, Default)]
pub struct MarketQuotes {
    pub up_orders: Vec<RestingOrder>,
    pub down_orders: Vec<RestingOrder>,
}

impl MarketQuotes {
    /// All orders for a given side (read-only)
    pub fn side_orders(&self, side: Side) -> &Vec<RestingOrder> {
        match side {
            Side::Up => &self.up_orders,
            Side::Down => &self.down_orders,
        }
    }

    /// All orders for a given side (mutable)
    pub fn side_orders_mut(&mut self, side: Side) -> &mut Vec<RestingOrder> {
        match side {
            Side::Up => &mut self.up_orders,
            Side::Down => &mut self.down_orders,
        }
    }

    /// Best (highest price) order on a side – keeps `log_status` working.
    pub fn order_for_side(&self, side: Side) -> Option<&RestingOrder> {
        self.side_orders(side)
            .iter()
            .max_by(|a, b| a.price.cmp(&b.price))
    }

    /// Find an order by id (read-only)
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

    /// Find an order by id (mutable)
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

    /// Remove a specific order by id
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
    pub trade_id: Option<String>,
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

    pub fn add_buy(&mut self, side: Side, size: Decimal, price: Decimal) {
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

#[derive(Debug, Clone, Default)]
pub struct TradingState {
    pub inventory: MarketInventory,
}

impl TradingState {
    pub fn imbalance(&self) -> Decimal {
        self.inventory.imbalance()
    }

    pub fn add_fill(&mut self, side: Side, size: Decimal, price: Decimal) {
        self.inventory.add_buy(side, size, price);
    }
}

#[derive(Debug, Default)]
pub struct ExecutionState {
    pub trading_state: TradingState,
    pub quotes: MarketQuotes,
    pub taker_in_flight: bool,
    pub last_taker_time: Option<Instant>,
    pub balance_error: bool,
    pub last_ladder_refresh: Option<Instant>,
    pub last_positions_update: Option<Instant>,
    pub up_bid: Option<Decimal>,
    pub up_ask: Option<Decimal>,
    pub down_bid: Option<Decimal>,
    pub down_ask: Option<Decimal>,
    pub processed_trade_ids: HashMap<String, Instant>,
    pub prefilled: HashMap<String, (Decimal, Instant)>,
}

impl ExecutionState {
    pub fn can_send_taker(
        &self,
        taker_cooldown: Duration,
        balance_error_cooldown: Duration,
    ) -> bool {
        if self.taker_in_flight {
            return false;
        }
        if let Some(t) = self.last_taker_time {
            if self.balance_error && t.elapsed() < balance_error_cooldown {
                return false;
            }
            if t.elapsed() < taker_cooldown {
                return false;
            }
        }
        true
    }

    pub fn inventory(&self) -> &MarketInventory {
        &self.trading_state.inventory
    }

    pub fn inventory_mut(&mut self) -> &mut MarketInventory {
        &mut self.trading_state.inventory
    }

    pub fn record_fill(&mut self, side: Side, size: Decimal, price: Decimal) {
        self.trading_state.add_fill(side, size, price);
    }

    pub fn ask_for_side(&self, side: Side) -> Option<Decimal> {
        match side {
            Side::Up => self.up_ask,
            Side::Down => self.down_ask,
        }
    }
}

#[derive(Clone)]
pub struct MarketLookup {
    condition_to_gamma: HashMap<String, String>,
    gamma_to_ids: HashMap<String, MarketIds>,
}

impl MarketLookup {
    pub fn new(markets: &HashMap<String, MarketState>) -> Self {
        let mut condition_to_gamma = HashMap::new();
        let mut gamma_to_ids = HashMap::new();

        for (gamma_id, state) in markets {
            let ids = &state.info.ids;
            condition_to_gamma.insert(ids.condition_id.clone(), gamma_id.clone());
            gamma_to_ids.insert(gamma_id.clone(), ids.clone());
        }

        Self {
            condition_to_gamma,
            gamma_to_ids,
        }
    }

    pub fn resolve_condition(&self, condition_id: &str) -> Option<&str> {
        self.condition_to_gamma
            .get(condition_id)
            .map(|s| s.as_str())
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
