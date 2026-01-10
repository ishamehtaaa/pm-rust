use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, warn};

use crate::models::MarketInfo;
use crate::balances::EffectiveBalances;
use crate::utils::{clamp_decimal, round_down_2dp, round_down_for_tick, round_up_2dp};

#[derive(Debug, Clone, Copy)]
pub enum MarketStage {
    Early,
    Mid,
    Late,
}

#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub up_bid: Decimal,
    pub up_ask: Decimal,
    pub down_bid: Decimal,
    pub down_ask: Decimal,
    pub up_min_size: Decimal,
    pub down_min_size: Decimal,
    pub tick_size: Decimal,
    pub up_momentum: Decimal,
    pub down_momentum: Decimal,
}

#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub reason: String,
    pub market_usdc: Option<Decimal>,
    pub kind: OrderKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKind {
    Ladder,
    Drift,
    Rebalance,
}

pub trait Strategy: Send + Sync {
    fn equilibrium_buffer(&self) -> Decimal;
    fn rebalance_threshold(&self) -> Decimal;
    fn stage_for_market(&self, now: DateTime<Utc>, market: &MarketInfo) -> MarketStage;
    fn target_total_shares(&self, now: DateTime<Utc>, market: &MarketInfo) -> Decimal;
    fn build_orders(
        &self,
        market: &MarketInfo,
        snapshot: &OrderBookSnapshot,
        stage: MarketStage,
        balances: EffectiveBalances,
    ) -> Vec<OrderIntent>;
    fn apply_share_limits(
        &self,
        orders: Vec<OrderIntent>,
        balances: EffectiveBalances,
        market: &MarketInfo,
    ) -> Vec<OrderIntent>;

    fn build_orders_with_result(
        &self,
        market: &MarketInfo,
        snapshot: &OrderBookSnapshot,
        stage: MarketStage,
        balances: EffectiveBalances,
    ) -> StrategyResult {
        let orders = self.build_orders(market, snapshot, stage, balances);
        let outcome = if orders.is_empty() {
            StrategyOutcome::Converged
        } else {
            StrategyOutcome::Generated
        };
        StrategyResult {
            orders,
            outcome,
            iterations: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyOutcome {
    Generated,
    Converged,
    NoLiquidity,
    MaxIterations,
}

#[derive(Debug, Clone)]
pub struct StrategyResult {
    pub orders: Vec<OrderIntent>,
    pub outcome: StrategyOutcome,
    pub iterations: u32,
}

#[derive(Debug, Clone)]
pub struct StrategyParams {
    pub base_size: Decimal,
    pub rebalance_threshold: Decimal,
    pub rebalance_chunk: Decimal,
    pub max_shares_per_side: Decimal,
    pub equilibrium_buffer: Decimal,
}

#[derive(Debug, Clone)]
pub struct LadderStrategy {
    base_size: Decimal,
    rebalance_threshold: Decimal,
    rebalance_chunk: Decimal,
    combined_cap: Decimal,
    equilibrium_buffer: Decimal,
    max_shares_per_side: Decimal,
}

impl LadderStrategy {
    pub fn new(params: StrategyParams) -> Self {
        Self {
            base_size: params.base_size,
            rebalance_threshold: params.rebalance_threshold,
            rebalance_chunk: params.rebalance_chunk,
            combined_cap: dec!(0.98),
            equilibrium_buffer: params.equilibrium_buffer,
            max_shares_per_side: params.max_shares_per_side,
        }
    }

    pub fn equilibrium_buffer(&self) -> Decimal {
        self.equilibrium_buffer
    }

    pub fn stage_for_market(&self, now: DateTime<Utc>, market: &MarketInfo) -> MarketStage {
        let remaining = market.end_time - now;
        let remaining_minutes = remaining.num_minutes();
        if remaining_minutes > 10 {
            MarketStage::Early
        } else if remaining_minutes > 5 {
            MarketStage::Mid
        } else {
            MarketStage::Late
        }
    }

    pub fn target_total_shares(&self, now: DateTime<Utc>, market: &MarketInfo) -> Decimal {
        // Scale total target inventory from 100 down to 10 as the market approaches end.
        let total_minutes = (market.end_time - market.start_time).num_minutes().max(1);
        let remaining_minutes = (market.end_time - now).num_minutes().max(0);
        if remaining_minutes >= total_minutes {
            return dec!(100.0);
        }
        if remaining_minutes <= 5 {
            return dec!(10.0);
        }

        let remaining = Decimal::from(remaining_minutes as u64);
        let total = Decimal::from(total_minutes as u64);
        let slope = (dec!(100.0) - dec!(10.0)) / (total - dec!(5.0));
        let interpolated = dec!(10.0) + slope * (remaining - dec!(5.0));
        interpolated.max(dec!(10.0)).min(dec!(100.0))
    }

    pub fn build_orders(
        &self,
        market: &MarketInfo,
        snapshot: &OrderBookSnapshot,
        stage: MarketStage,
        balances: EffectiveBalances,
    ) -> Vec<OrderIntent> {
        // Size rungs by base_size with a decay factor, honoring min order sizes.
        let target_total = self
            .target_total_shares(Utc::now(), market)
            .min(self.max_shares_per_side * dec!(2.0));
        let target_side = target_total / dec!(2.0);
        let min_notional = dec!(1.0);
        let min_shares_up = if snapshot.up_bid > Decimal::ZERO {
            round_up_2dp(min_notional / snapshot.up_bid)
        } else {
            Decimal::ZERO
        };
        let min_shares_down = if snapshot.down_bid > Decimal::ZERO {
            round_up_2dp(min_notional / snapshot.down_bid)
        } else {
            Decimal::ZERO
        };
        let effective_target_side = target_side.max(min_shares_up).max(min_shares_down);
        let equilibrium_buffer = self.equilibrium_buffer;
        if (balances.up - target_side).abs() <= equilibrium_buffer
            && (balances.down - target_side).abs() <= equilibrium_buffer
        {
            return Vec::new();
        }
        if balances.up >= effective_target_side + equilibrium_buffer
            && balances.down >= effective_target_side + equilibrium_buffer
        {
            return Vec::new();
        }
        let rungs = match stage {
            MarketStage::Early => 6,
            MarketStage::Mid => 4,
            MarketStage::Late => 2,
        };

        let spread_up = snapshot.up_ask - snapshot.up_bid;
        let spread_down = snapshot.down_ask - snapshot.down_bid;
        let avg_spread = (spread_up + spread_down) / dec!(2.0);
        let step = clamp_decimal(avg_spread / dec!(2.0), dec!(0.01), dec!(0.05));

        let mut orders = Vec::new();
        let mut weight = dec!(1.0);
        let weight_decay = dec!(0.75);

        for rung in 0..rungs {
            let offset = step * Decimal::from(rung as u32);
            let mut up_price = snapshot.up_bid - offset;
            let mut down_price = snapshot.down_bid - offset;

            if up_price <= dec!(0.0) || down_price <= dec!(0.0) {
                break;
            }

            let combined_cap = self.combined_cap;
            if up_price + down_price > combined_cap {
                let excess = up_price + down_price - combined_cap;
                let adjust = excess / dec!(2.0);
                up_price = (up_price - adjust).max(snapshot.tick_size);
                down_price = (down_price - adjust).max(snapshot.tick_size);
            }

            if down_price <= dec!(0.0) {
                continue;
            }

            up_price = round_down_for_tick(up_price.max(snapshot.tick_size), snapshot.tick_size);
            down_price =
                round_down_for_tick(down_price.max(snapshot.tick_size), snapshot.tick_size);

            let min_size = snapshot.up_min_size.max(snapshot.down_min_size);
            let size = round_down_2dp((self.base_size * weight).max(min_size));
            weight *= weight_decay;

            if size < snapshot.up_min_size || size < snapshot.down_min_size {
                debug!(
                    market = %market.slug,
                    size = %size,
                    up_min = %snapshot.up_min_size,
                    down_min = %snapshot.down_min_size,
                    "Skipping rung due to min order size"
                );
                continue;
            }

            orders.push(OrderIntent {
                token_id: market.ids.up_token.clone(),
                price: up_price,
                size,
                reason: format!("ladder_rung_{rung}"),
                market_usdc: None,
                kind: OrderKind::Ladder,
            });

            orders.push(OrderIntent {
                token_id: market.ids.down_token.clone(),
                price: down_price,
                size,
                reason: format!("ladder_rung_{rung}"),
                market_usdc: None,
                kind: OrderKind::Ladder,
            });
        }

        let up_delta = effective_target_side - balances.up;
        let down_delta = effective_target_side - balances.down;
        let drift_threshold = snapshot.up_min_size.max(snapshot.down_min_size);

        if up_delta.abs() >= drift_threshold {
            let price = snapshot.up_bid;
            let mut size =
                round_down_2dp(drift_threshold.max(self.rebalance_chunk.min(up_delta.abs())));
            let price = round_down_for_tick(price.max(snapshot.tick_size), snapshot.tick_size);
            let notional = size * price;
            let market_usdc = if up_delta.is_sign_positive() && notional < min_notional {
                size = round_up_2dp(min_notional / price);
                Some(min_notional)
            } else {
                None
            };

            orders.push(OrderIntent {
                token_id: market.ids.up_token.clone(),
                price,
                size,
                reason: "drift_correction_up".to_string(),
                market_usdc,
                kind: OrderKind::Drift,
            });
        }

        if down_delta.abs() >= drift_threshold {
            let price = snapshot.down_bid;
            let mut size =
                round_down_2dp(drift_threshold.max(self.rebalance_chunk.min(down_delta.abs())));
            let price = round_down_for_tick(price.max(snapshot.tick_size), snapshot.tick_size);
            let notional = size * price;
            let market_usdc = if down_delta.is_sign_positive() && notional < min_notional {
                size = round_up_2dp(min_notional / price);
                Some(min_notional)
            } else {
                None
            };

            orders.push(OrderIntent {
                token_id: market.ids.down_token.clone(),
                price,
                size,
                reason: "drift_correction_down".to_string(),
                market_usdc,
                kind: OrderKind::Drift,
            });
        }

        let imbalance = balances.up - balances.down;
        if imbalance.abs() >= self.rebalance_threshold {
            let (token_id, price) = if imbalance > dec!(0.0) {
                (market.ids.down_token.clone(), snapshot.down_bid)
            } else {
                (market.ids.up_token.clone(), snapshot.up_bid)
            };

            let rebalance_size =
                round_down_2dp(drift_threshold.max(self.rebalance_chunk.min(imbalance.abs())));
            let price = round_down_for_tick(price.max(snapshot.tick_size), snapshot.tick_size);
            let min_size = if token_id == market.ids.up_token {
                snapshot.up_min_size
            } else {
                snapshot.down_min_size
            };

            if rebalance_size >= min_size {
                orders.push(OrderIntent {
                    token_id,
                    price,
                    size: rebalance_size,
                    reason: "rebalance_top_of_book".to_string(),
                    market_usdc: None,
                    kind: OrderKind::Rebalance,
                });
            } else {
                warn!(
                    market = %market.slug,
                    size = %rebalance_size,
                    "Rebalance size below min order size"
                );
            }
        }

        orders
    }

    pub fn apply_share_limits(
        &self,
        orders: Vec<OrderIntent>,
        balances: EffectiveBalances,
        market: &MarketInfo,
    ) -> Vec<OrderIntent> {
        let mut projected_up = balances.up;
        let mut projected_down = balances.down;
        let mut filtered = Vec::new();

        for mut order in orders {
            let available = if order.token_id == market.ids.up_token {
                (self.max_shares_per_side - projected_up).max(Decimal::ZERO)
            } else {
                (self.max_shares_per_side - projected_down).max(Decimal::ZERO)
            };

            if available <= Decimal::ZERO {
                continue;
            }

            if order.size > available {
                order.size = round_down_2dp(available);
                if order.size.is_zero() {
                    continue;
                }
            }

            if order.token_id == market.ids.up_token {
                projected_up += order.size;
            } else {
                projected_down += order.size;
            }

            filtered.push(order);
        }

        filtered
    }
}

impl Strategy for LadderStrategy {
    fn equilibrium_buffer(&self) -> Decimal {
        self.equilibrium_buffer()
    }

    fn rebalance_threshold(&self) -> Decimal {
        self.rebalance_threshold
    }

    fn stage_for_market(&self, now: DateTime<Utc>, market: &MarketInfo) -> MarketStage {
        self.stage_for_market(now, market)
    }

    fn target_total_shares(&self, now: DateTime<Utc>, market: &MarketInfo) -> Decimal {
        self.target_total_shares(now, market)
    }

    fn build_orders(
        &self,
        market: &MarketInfo,
        snapshot: &OrderBookSnapshot,
        stage: MarketStage,
        balances: EffectiveBalances,
    ) -> Vec<OrderIntent> {
        self.build_orders(market, snapshot, stage, balances)
    }

    fn apply_share_limits(
        &self,
        orders: Vec<OrderIntent>,
        balances: EffectiveBalances,
        market: &MarketInfo,
    ) -> Vec<OrderIntent> {
        self.apply_share_limits(orders, balances, market)
    }
}

#[derive(Debug, Clone)]
pub struct SportsStrategy {
    inner: LadderStrategy,
}

impl SportsStrategy {
    pub fn new(params: StrategyParams) -> Self {
        Self {
            inner: LadderStrategy::new(params),
        }
    }
}

impl Strategy for SportsStrategy {
    fn equilibrium_buffer(&self) -> Decimal {
        self.inner.equilibrium_buffer()
    }

    fn rebalance_threshold(&self) -> Decimal {
        self.inner.rebalance_threshold
    }

    fn stage_for_market(&self, now: DateTime<Utc>, market: &MarketInfo) -> MarketStage {
        self.inner.stage_for_market(now, market)
    }

    fn target_total_shares(&self, now: DateTime<Utc>, market: &MarketInfo) -> Decimal {
        self.inner.target_total_shares(now, market)
    }

    fn build_orders(
        &self,
        market: &MarketInfo,
        snapshot: &OrderBookSnapshot,
        stage: MarketStage,
        balances: EffectiveBalances,
    ) -> Vec<OrderIntent> {
        build_sports_orders(self, market, snapshot, stage, balances)
    }

    fn apply_share_limits(
        &self,
        orders: Vec<OrderIntent>,
        balances: EffectiveBalances,
        market: &MarketInfo,
    ) -> Vec<OrderIntent> {
        self.inner
            .apply_share_limits(orders, balances, market)
    }
}

fn build_sports_orders(
    strategy: &SportsStrategy,
    market: &MarketInfo,
    snapshot: &OrderBookSnapshot,
    stage: MarketStage,
    balances: EffectiveBalances,
) -> Vec<OrderIntent> {
    let target_total = strategy
        .inner
        .target_total_shares(Utc::now(), market)
        .min(strategy.inner.max_shares_per_side * dec!(2.0));
    let target_side = target_total / dec!(2.0);
    let equilibrium_buffer = strategy.inner.equilibrium_buffer;
    if (balances.up - target_side).abs() <= equilibrium_buffer
        && (balances.down - target_side).abs() <= equilibrium_buffer
    {
        return Vec::new();
    }

    let min_notional = dec!(1.0);
    let min_shares_up = if snapshot.up_bid > Decimal::ZERO {
        round_up_2dp(min_notional / snapshot.up_bid)
    } else {
        Decimal::ZERO
    };
    let min_shares_down = if snapshot.down_bid > Decimal::ZERO {
        round_up_2dp(min_notional / snapshot.down_bid)
    } else {
        Decimal::ZERO
    };
    let effective_target_side = target_side.max(min_shares_up).max(min_shares_down);
    if balances.up >= effective_target_side + equilibrium_buffer
        && balances.down >= effective_target_side + equilibrium_buffer
    {
        return Vec::new();
    }

    let (preferred_token, preferred_bid, other_token, other_bid) =
        choose_preferred_side(market, snapshot);
    let tick = snapshot.tick_size;
    let rungs = match stage {
        MarketStage::Early => 2,
        MarketStage::Mid => 2,
        MarketStage::Late => 1,
    };

    let mut orders = Vec::new();
    for rung in 0..rungs {
        let rung_offset = Decimal::from(rung as u32);
        let preferred_price = round_down_for_tick(
            (preferred_bid - tick * (rung_offset + dec!(1.0))).max(tick),
            tick,
        );
        let other_price = round_down_for_tick(
            (other_bid - tick * (rung_offset + dec!(2.0))).max(tick),
            tick,
        );

        let min_size = snapshot.up_min_size.max(snapshot.down_min_size);
        let base_size = strategy.inner.base_size.max(min_size);
        let preferred_size = round_down_2dp(base_size);
        let other_size = round_down_2dp(base_size * dec!(0.8));

        orders.push(OrderIntent {
            token_id: preferred_token.clone(),
            price: preferred_price,
            size: preferred_size,
            reason: format!("ladder_rung_{rung}"),
            market_usdc: None,
            kind: OrderKind::Ladder,
        });

        orders.push(OrderIntent {
            token_id: other_token.clone(),
            price: other_price,
            size: other_size,
            reason: format!("ladder_rung_{rung}"),
            market_usdc: None,
            kind: OrderKind::Ladder,
        });
    }

    let up_delta = effective_target_side - balances.up;
    let down_delta = effective_target_side - balances.down;
    let drift_threshold = snapshot.up_min_size.max(snapshot.down_min_size);

    if up_delta.abs() >= drift_threshold {
        let price = snapshot.up_bid;
        let mut size = round_down_2dp(
            drift_threshold.max(strategy.inner.rebalance_chunk.min(up_delta.abs())),
        );
        let price = round_down_for_tick(price.max(snapshot.tick_size), snapshot.tick_size);
        let notional = size * price;
        let market_usdc = if up_delta.is_sign_positive() && notional < min_notional {
            size = round_up_2dp(min_notional / price);
            Some(min_notional)
        } else {
            None
        };

        orders.push(OrderIntent {
            token_id: market.ids.up_token.clone(),
            price,
            size,
            reason: "drift_correction_up".to_string(),
            market_usdc,
            kind: OrderKind::Drift,
        });
    }

    if down_delta.abs() >= drift_threshold {
        let price = snapshot.down_bid;
        let mut size = round_down_2dp(
            drift_threshold.max(strategy.inner.rebalance_chunk.min(down_delta.abs())),
        );
        let price = round_down_for_tick(price.max(snapshot.tick_size), snapshot.tick_size);
        let notional = size * price;
        let market_usdc = if down_delta.is_sign_positive() && notional < min_notional {
            size = round_up_2dp(min_notional / price);
            Some(min_notional)
        } else {
            None
        };

        orders.push(OrderIntent {
            token_id: market.ids.down_token.clone(),
            price,
            size,
            reason: "drift_correction_down".to_string(),
            market_usdc,
            kind: OrderKind::Drift,
        });
    }

    orders
}

fn choose_preferred_side(
    market: &MarketInfo,
    snapshot: &OrderBookSnapshot,
) -> (String, Decimal, String, Decimal) {
    let up_mom = snapshot.up_momentum;
    let down_mom = snapshot.down_momentum;
    if up_mom > down_mom {
        (
            market.ids.up_token.clone(),
            snapshot.up_bid,
            market.ids.down_token.clone(),
            snapshot.down_bid,
        )
    } else if down_mom > up_mom {
        (
            market.ids.down_token.clone(),
            snapshot.down_bid,
            market.ids.up_token.clone(),
            snapshot.up_bid,
        )
    } else if snapshot.up_bid >= snapshot.down_bid {
        (
            market.ids.up_token.clone(),
            snapshot.up_bid,
            market.ids.down_token.clone(),
            snapshot.down_bid,
        )
    } else {
        (
            market.ids.down_token.clone(),
            snapshot.down_bid,
            market.ids.up_token.clone(),
            snapshot.up_bid,
        )
    }
}
