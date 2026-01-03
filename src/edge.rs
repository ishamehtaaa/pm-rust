use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub asset: String,
    pub market_id: String,
    pub open_price: Decimal,
    pub current_price: Decimal,
    pub pm_up_ask: Decimal,
    pub pm_down_ask: Decimal,
    pub elapsed_pct: f64,
}

impl MarketSnapshot {
    pub fn chainlink_direction(&self) -> Direction {
        if self.current_price > self.open_price { Direction::Up } 
        else if self.current_price < self.open_price { Direction::Down } 
        else { Direction::Neutral }
    }
    pub fn price_move_pct(&self) -> Decimal {
        if self.open_price.is_zero() { return Decimal::ZERO; }
        (self.current_price - self.open_price) / self.open_price
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction { Up, Down, Neutral }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Up, Down }

#[derive(Debug, Clone)]
pub struct BuySignal {
    pub asset: String,
    pub market_id: String,
    pub side: Side,
    pub price: Decimal,
    pub reason: BuyReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuyReason {
    CheapCorrectSide,
    CheapOppositeSide,
    EmergencyMop,
}

pub struct ArbDetector {
    pub max_first_leg_price: Decimal,
    pub max_combined_cost: Decimal,
    pub min_move_pct: Decimal,
    pub min_elapsed_pct: f64,
    pub mop_threshold: Decimal, 
}

impl Default for ArbDetector {
    fn default() -> Self {
        Self {
            max_first_leg_price: dec!(0.75), // Allows ETH Down at 0.66
            max_combined_cost: dec!(0.98),   // Allows Async Arb capture
            min_move_pct: dec!(0.002),
            min_elapsed_pct: 0.10,
            mop_threshold: dec!(1.01),      // Breakeven/Safety floor
        }
    }
}

impl ArbDetector {
    pub fn check_first_leg(&self, snap: &MarketSnapshot) -> Option<BuySignal> {
        // Check elapsed
        let dir = snap.chainlink_direction();
        let (side, ask) = match dir {
            Direction::Up => (Side::Up, snap.pm_up_ask),
            Direction::Down => (Side::Down, snap.pm_down_ask),
            Direction::Neutral => return None,
        };
        let move_pct = snap.price_move_pct().abs();
        // Ensure ask > 0 to filter out invalid/stale data
        if ask <= dec!(0) {
            tracing::debug!("skip_first {}: ask<=0 (ask={})", snap.market_id, ask);
            return None;
        }
        tracing::info!("first_leg_signal {} side={:?} ask={} move_pct={}", snap.market_id, side, ask, move_pct);
        Some(BuySignal { asset: snap.asset.clone(), market_id: snap.market_id.clone(), side, price: ask, reason: BuyReason::CheapCorrectSide })
    }

    pub fn check_second_leg(&self, snap: &MarketSnapshot, held_side: Side, held_price: Decimal) -> Option<BuySignal> {
        let (side, ask) = match held_side { Side::Up => (Side::Down, snap.pm_down_ask), Side::Down => (Side::Up, snap.pm_up_ask) };
        if ask <= dec!(0) {
            tracing::debug!("skip_second {}: ask<=0 (ask={})", snap.market_id, ask);
            return None;
        }
        let combined = held_price + ask;
        if combined <= self.max_combined_cost {
            tracing::info!("second_leg_signal {} side={:?} held_price={} ask={} combined={}", snap.market_id, side, held_price, ask, combined);
            return Some(BuySignal { asset: snap.asset.clone(), market_id: snap.market_id.clone(), side, price: ask, reason: BuyReason::CheapOppositeSide });
        }
        tracing::debug!("skip_second {}: combined {} > max_combined_cost {}", snap.market_id, combined, self.max_combined_cost);
        None
    }

    pub fn check_emergency_mop(&self, snap: &MarketSnapshot, held_side: Side, held_price: Decimal) -> Option<BuySignal> {
        let (side, ask) = match held_side { Side::Up => (Side::Down, snap.pm_down_ask), Side::Down => (Side::Up, snap.pm_up_ask) };
        if ask > dec!(0) && (held_price + ask) <= self.mop_threshold {
            return Some(BuySignal { asset: snap.asset.clone(), market_id: snap.market_id.clone(), side, price: ask, reason: BuyReason::EmergencyMop });
        }
        None
    }
}