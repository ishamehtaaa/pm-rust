use crate::models::Side;
use rust_decimal::Decimal;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct MarketInventory {
    pub up_shares: Decimal,
    pub down_shares: Decimal,
    pub up_avg: Option<Decimal>,
    pub down_avg: Option<Decimal>,
}

impl MarketInventory {
    pub fn imbalance(&self) -> Decimal {
        self.up_shares - self.down_shares
    }
}

#[derive(Debug, Clone)]
pub struct RestingOrder {
    pub order_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub placed_at: Instant,
}

#[derive(Debug, Default)]
pub struct MarketOrders {
    pub up: Vec<RestingOrder>,
    pub down: Vec<RestingOrder>,
    pub last_refresh: Option<Instant>,
}

impl MarketOrders {
    pub fn side_orders(&self, side: Side) -> &Vec<RestingOrder> {
        match side {
            Side::Up => &self.up,
            Side::Down => &self.down,
        }
    }

    pub fn side_orders_mut(&mut self, side: Side) -> &mut Vec<RestingOrder> {
        match side {
            Side::Up => &mut self.up,
            Side::Down => &mut self.down,
        }
    }

    pub fn pending_shares(&self, side: Side) -> Decimal {
        self.side_orders(side).iter().map(|o| o.size).sum()
    }

    pub fn optimistic_shares(&self, side: Side, delay: Duration) -> Decimal {
        let now = Instant::now();
        self.side_orders(side)
            .iter()
            .filter(|o| now.duration_since(o.placed_at) >= delay)
            .map(|o| o.size)
            .sum()
    }
}
