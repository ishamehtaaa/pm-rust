// arb_finder/signals.rs
//
// Signal detection rules for predicting arbitrage opportunities.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rust_decimal::prelude::FromPrimitive;
use std::time::{Duration, Instant};

use chrono::Utc;
use super::config::ArbFinderConfig;
use super::market_state::{MarketState, ThinSide, TradeSide};

/// A detected signal that may indicate an upcoming arb opportunity
#[derive(Debug, Clone)]
pub struct Signal {
    pub signal_type: SignalType,
    pub strength: Decimal,
    pub timestamp: Instant,
    pub details: SignalDetails,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SignalType {
    /// Large trade detected (potential sweep)
    Sweep,
    /// Order book imbalance detected
    Imbalance,
    /// Rapid price movement on one side
    Velocity,
    /// REST/WS price discrepancy
    Discrepancy,
    /// Composite signal (momentum + volatility + depth + time)
    Composite,
}

#[derive(Debug, Clone)]
pub enum SignalDetails {
    Sweep {
        token_id: String,
        trade_side: TradeSide,
        trade_size: Decimal,
        average_size: Decimal,
    },
    Imbalance {
        thin_side: ThinSide,
        ratio: Decimal,
    },
    Velocity {
        lagging_side: ThinSide,
        fast_velocity: Decimal,
        slow_velocity: Decimal,
    },
    Discrepancy {
        token_id: String,
        rest_price: Decimal,
        ws_price: Decimal,
        diff: Decimal,
    },
    Composite {
        score: Decimal,
        momentum: Decimal,
        volatility: Decimal,
        imbalance: Decimal,
        time_to_end_secs: i64,
    },
}

/// Signal detector that analyzes market state
pub struct SignalDetector {
    config: ArbFinderConfig,
}

impl SignalDetector {
    pub fn new(config: ArbFinderConfig) -> Self {
        Self { config }
    }

    /// Analyze market state and return all detected signals
    pub fn detect_signals(&self, state: &MarketState) -> Vec<Signal> {
        let mut signals = Vec::new();

        if let Some(signal) = self.detect_sweep(state) {
            signals.push(signal);
        }

        if let Some(signal) = self.detect_imbalance(state) {
            signals.push(signal);
        }

        if let Some(signal) = self.detect_velocity(state) {
            signals.push(signal);
        }

        if let Some(signal) = self.detect_discrepancy_up(state) {
            signals.push(signal);
        }

        if let Some(signal) = self.detect_discrepancy_down(state) {
            signals.push(signal);
        }

        if let Some(signal) = self.detect_composite(state) {
            signals.push(signal);
        }

        signals
    }

    /// Detect sweep signal (large trade on one side)
    fn detect_sweep(&self, state: &MarketState) -> Option<Signal> {
        let window = Duration::from_secs(5);

        // Check Up token for large trades
        if let Some(trade) = state.up.largest_recent_trade(window) {
            if trade.size >= self.config.sweep_size_threshold {
                let avg_volume = state.up.recent_trade_volume(Duration::from_secs(60))
                    / dec!(12); // Rough average trade size

                let strength = if avg_volume > Decimal::ZERO {
                    (trade.size / avg_volume).min(dec!(1.0))
                } else {
                    dec!(0.5)
                };

                return Some(Signal {
                    signal_type: SignalType::Sweep,
                    strength,
                    timestamp: Instant::now(),
                    details: SignalDetails::Sweep {
                        token_id: state.up_token_id.clone(),
                        trade_side: trade.side,
                        trade_size: trade.size,
                        average_size: avg_volume,
                    },
                });
            }
        }

        // Check Down token for large trades
        if let Some(trade) = state.down.largest_recent_trade(window) {
            if trade.size >= self.config.sweep_size_threshold {
                let avg_volume = state.down.recent_trade_volume(Duration::from_secs(60))
                    / dec!(12);

                let strength = if avg_volume > Decimal::ZERO {
                    (trade.size / avg_volume).min(dec!(1.0))
                } else {
                    dec!(0.5)
                };

                return Some(Signal {
                    signal_type: SignalType::Sweep,
                    strength,
                    timestamp: Instant::now(),
                    details: SignalDetails::Sweep {
                        token_id: state.down_token_id.clone(),
                        trade_side: trade.side,
                        trade_size: trade.size,
                        average_size: avg_volume,
                    },
                });
            }
        }

        None
    }

    /// Detect order book imbalance
    fn detect_imbalance(&self, state: &MarketState) -> Option<Signal> {
        let ratio = state.depth_imbalance_ratio()?;
        let thin_side = state.thin_side()?;

        // Check if imbalance exceeds threshold
        let effective_ratio = if thin_side == ThinSide::Down {
            ratio
        } else {
            dec!(1.0) / ratio
        };

        if effective_ratio < self.config.imbalance_ratio_threshold {
            return None;
        }

        // Strength based on how extreme the imbalance is
        let strength = ((effective_ratio - dec!(1.0)) / self.config.imbalance_ratio_threshold)
            .min(dec!(1.0));

        Some(Signal {
            signal_type: SignalType::Imbalance,
            strength,
            timestamp: Instant::now(),
            details: SignalDetails::Imbalance {
                thin_side,
                ratio: effective_ratio,
            },
        })
    }

    /// Detect velocity divergence (one side moving faster than the other)
    fn detect_velocity(&self, state: &MarketState) -> Option<Signal> {
        let up_velocity = state.up.price_velocity(self.config.velocity_window)?;
        let down_velocity = state.down.price_velocity(self.config.velocity_window)?;

        let up_abs = up_velocity.abs();
        let down_abs = down_velocity.abs();

        // Check if one side is moving significantly faster
        let (fast_side, fast_vel, slow_vel) = if up_abs > down_abs {
            (ThinSide::Up, up_abs, down_abs)
        } else {
            (ThinSide::Down, down_abs, up_abs)
        };

        // Need meaningful velocity
        if fast_vel < self.config.velocity_threshold {
            return None;
        }

        // The lagging side is the opposite of the fast side
        let lagging_side = if fast_side == ThinSide::Up {
            ThinSide::Down
        } else {
            ThinSide::Up
        };

        // Strength based on velocity difference
        let velocity_ratio = if slow_vel > Decimal::ZERO {
            fast_vel / slow_vel
        } else {
            dec!(2.0)
        };

        if velocity_ratio < dec!(1.5) {
            return None;
        }

        let strength = ((velocity_ratio - dec!(1.0)) / dec!(2.0)).min(dec!(1.0));

        Some(Signal {
            signal_type: SignalType::Velocity,
            strength,
            timestamp: Instant::now(),
            details: SignalDetails::Velocity {
                lagging_side,
                fast_velocity: fast_vel,
                slow_velocity: slow_vel,
            },
        })
    }

    /// Detect REST/WS discrepancy for Up token
    fn detect_discrepancy_up(&self, state: &MarketState) -> Option<Signal> {
        let discrepancy = state.up.rest_ws_discrepancy()?;

        if discrepancy < self.config.rest_ws_discrepancy_threshold {
            return None;
        }

        let rest_price = state.up.rest_ask?;
        let ws_price = state.up.ws_ask?;

        let strength = (discrepancy / self.config.rest_ws_discrepancy_threshold / dec!(3.0))
            .min(dec!(1.0));

        Some(Signal {
            signal_type: SignalType::Discrepancy,
            strength,
            timestamp: Instant::now(),
            details: SignalDetails::Discrepancy {
                token_id: state.up_token_id.clone(),
                rest_price,
                ws_price,
                diff: discrepancy,
            },
        })
    }

    /// Detect REST/WS discrepancy for Down token
    fn detect_discrepancy_down(&self, state: &MarketState) -> Option<Signal> {
        let discrepancy = state.down.rest_ws_discrepancy()?;

        if discrepancy < self.config.rest_ws_discrepancy_threshold {
            return None;
        }

        let rest_price = state.down.rest_ask?;
        let ws_price = state.down.ws_ask?;

        let strength = (discrepancy / self.config.rest_ws_discrepancy_threshold / dec!(3.0))
            .min(dec!(1.0));

        Some(Signal {
            signal_type: SignalType::Discrepancy,
            strength,
            timestamp: Instant::now(),
            details: SignalDetails::Discrepancy {
                token_id: state.down_token_id.clone(),
                rest_price,
                ws_price,
                diff: discrepancy,
            },
        })
    }

    fn detect_composite(&self, state: &MarketState) -> Option<Signal> {
        let momentum_up = state
            .up
            .rest_ema_slope(Duration::from_secs(3), Duration::from_secs(12))
            .unwrap_or(Decimal::ZERO)
            .abs();
        let momentum_down = state
            .down
            .rest_ema_slope(Duration::from_secs(3), Duration::from_secs(12))
            .unwrap_or(Decimal::ZERO)
            .abs();
        let momentum = (momentum_up + momentum_down) / dec!(2.0);

        let vol_up = state
            .up
            .rest_price_range(Duration::from_secs(10))
            .unwrap_or(Decimal::ZERO);
        let vol_down = state
            .down
            .rest_price_range(Duration::from_secs(10))
            .unwrap_or(Decimal::ZERO);
        let volatility = (vol_up + vol_down) / dec!(2.0);

        let imbalance_ratio = state.depth_imbalance_ratio().unwrap_or(Decimal::ONE);
        let imbalance = if imbalance_ratio >= Decimal::ONE {
            imbalance_ratio - Decimal::ONE
        } else {
            (Decimal::ONE / imbalance_ratio) - Decimal::ONE
        };

        let time_to_end_secs = (state.end_time - Utc::now()).num_seconds();

        let momentum_score = (momentum / dec!(0.02)).min(Decimal::ONE);
        let volatility_score = (volatility / dec!(0.03)).min(Decimal::ONE);
        let imbalance_score = (imbalance / self.config.imbalance_ratio_threshold)
            .min(Decimal::ONE);
        let time_score = if time_to_end_secs <= 0 {
            Decimal::ONE
        } else if time_to_end_secs <= 300 {
            let secs = Decimal::from_i64(time_to_end_secs).unwrap_or(Decimal::ZERO);
            Decimal::ONE - (secs / dec!(300))
        } else {
            Decimal::ZERO
        };

        let score = (momentum_score + volatility_score + imbalance_score + time_score) / dec!(4.0);

        if score < self.config.composite_signal_threshold {
            return None;
        }

        Some(Signal {
            signal_type: SignalType::Composite,
            strength: score,
            timestamp: Instant::now(),
            details: SignalDetails::Composite {
                score,
                momentum,
                volatility,
                imbalance,
                time_to_end_secs,
            },
        })
    }
}
