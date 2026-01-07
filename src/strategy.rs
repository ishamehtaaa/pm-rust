use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, warn};

use crate::models::MarketInfo;
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
}

#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub reason: String,
    pub market_usdc: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct LadderStrategy {
    base_size: Decimal,
    rebalance_threshold: Decimal,
}

impl LadderStrategy {
    pub fn new(base_size: Decimal, rebalance_threshold: Decimal) -> Self {
        Self {
            base_size,
            rebalance_threshold,
        }
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
        up_balance: Decimal,
        down_balance: Decimal,
    ) -> Vec<OrderIntent> {
        let target_total = self.target_total_shares(Utc::now(), market);
        let target_side = target_total / dec!(2.0);
        let equilibrium_buffer = dec!(5.0);
        if (up_balance - target_side).abs() <= equilibrium_buffer
            && (down_balance - target_side).abs() <= equilibrium_buffer
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

            let combined_cap = dec!(0.99);
            if up_price + down_price > combined_cap {
                down_price = combined_cap - up_price;
            }

            if down_price <= dec!(0.0) {
                continue;
            }

            up_price = round_down_for_tick(up_price.max(snapshot.tick_size), snapshot.tick_size);
            down_price = round_down_for_tick(down_price.max(snapshot.tick_size), snapshot.tick_size);

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
            });

            orders.push(OrderIntent {
                token_id: market.ids.down_token.clone(),
                price: down_price,
                size,
                reason: format!("ladder_rung_{rung}"),
                market_usdc: None,
            });
        }

        let up_delta = effective_target_side - up_balance;
        let down_delta = effective_target_side - down_balance;
        let drift_threshold = snapshot
            .up_min_size
            .max(snapshot.down_min_size);

        if up_delta.abs() >= drift_threshold {
            let price = if up_delta.is_sign_positive() {
                snapshot.up_bid
            } else {
                snapshot.up_ask
            };
            let mut size = round_down_2dp(up_delta.abs().min(drift_threshold));
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
            });
        }

        if down_delta.abs() >= drift_threshold {
            let price = if down_delta.is_sign_positive() {
                snapshot.down_bid
            } else {
                snapshot.down_ask
            };
            let mut size = round_down_2dp(down_delta.abs().min(drift_threshold));
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
            });
        }

        let imbalance = up_balance - down_balance;
        if imbalance.abs() >= self.rebalance_threshold {
            let (token_id, price) = if imbalance > dec!(0.0) {
                (market.ids.down_token.clone(), snapshot.down_bid)
            } else {
                (market.ids.up_token.clone(), snapshot.up_bid)
            };

            let rebalance_size = round_down_2dp(imbalance.abs().min(dec!(5.0)));
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
}
