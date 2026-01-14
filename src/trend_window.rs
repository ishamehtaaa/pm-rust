use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::VecDeque;

use crate::rn_jd::{self, MarketParams};

const RNJD_MC_SAMPLES: usize = 200;
const RNJD_RISK_AVERSION: f64 = 0.01;
const RNJD_TIME_HORIZON: f64 = 30.0;
const RNJD_K_LIQUIDITY: f64 = 1.5;

#[derive(Debug)]
pub struct TrendWindow {
    samples: VecDeque<TrendSample>,
    first_seen_ms: Option<i64>,
    last_seen_ms: Option<i64>,
    min_up_bid: VecDeque<WindowPoint>,
    max_up_bid: VecDeque<WindowPoint>,
    min_up_ask: VecDeque<WindowPoint>,
    max_up_ask: VecDeque<WindowPoint>,
    min_down_bid: VecDeque<WindowPoint>,
    max_down_bid: VecDeque<WindowPoint>,
    min_down_ask: VecDeque<WindowPoint>,
    max_down_ask: VecDeque<WindowPoint>,
    rn_jd_params: MarketParams,
    first_mid: Option<f64>,
    last_mid: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
struct TrendSample {
    ts_ms: i64,
    up_bid: Decimal,
    up_ask: Decimal,
    down_bid: Decimal,
    down_ask: Decimal,
    logit_mid: f64,
}

#[derive(Debug, Clone, Copy)]
struct WindowPoint {
    ts_ms: i64,
    value: Decimal,
}

impl TrendWindow {
    pub fn record(
        &mut self,
        ts_ms: i64,
        up_bid: Decimal,
        up_ask: Decimal,
        down_bid: Decimal,
        down_ask: Decimal,
        window_secs: u64,
    ) {
        let window_ms = (window_secs as i64) * 1000;
        let mid_prob = decimal_mid_prob(up_bid, up_ask);
        let logit_mid = rn_jd::logit(mid_prob);
        if self.first_seen_ms.is_none() {
            self.first_seen_ms = Some(ts_ms);
            self.first_mid = Some(mid_prob);
        }
        self.last_seen_ms = Some(ts_ms);
        self.last_mid = Some(mid_prob);
        self.samples.push_back(TrendSample {
            ts_ms,
            up_bid,
            up_ask,
            down_bid,
            down_ask,
            logit_mid,
        });
        self.push_min_max(ts_ms, up_bid, up_ask, down_bid, down_ask);
        self.prune(window_ms, ts_ms);
    }

    pub fn is_ready(&self, window_secs: u64) -> bool {
        let window_ms = (window_secs as i64) * 1000;
        match (self.first_seen_ms, self.last_seen_ms) {
            (Some(first), Some(last)) => last - first >= window_ms,
            _ => false,
        }
    }

    pub fn is_within_range(&self, max_range: Decimal) -> bool {
        let ranges = self.ranges();
        ranges.up_bid <= max_range
            && ranges.up_ask <= max_range
            && ranges.down_bid <= max_range
            && ranges.down_ask <= max_range
    }

    pub fn span_ms_and_count(&self) -> (i64, usize) {
        let count = self.samples.len();
        let span_ms = match (self.first_seen_ms, self.last_seen_ms) {
            (Some(first), Some(last)) => last - first,
            _ => 0,
        };
        (span_ms, count)
    }

    pub fn mid_start_end(&self) -> Option<(f64, f64)> {
        match (self.first_mid, self.last_mid) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => None,
        }
    }

    /// Calculate volatility metrics for adaptive spread sizing.
    /// This captures "how fast" prices are moving, not just range.
    pub fn volatility_metrics(&self) -> VolatilityMetrics {
        if self.samples.len() < 3 {
            return VolatilityMetrics::default();
        }

        // Calculate realized volatility from log-odds increments
        let mut sum_sq = 0.0;
        let mut sum_change = 0.0;
        let mut weight_sum = 0.0;
        let mut weighted_direction = 0.0;
        
        let samples: Vec<_> = self.samples.iter().collect();
        let n = samples.len();
        
        for i in 1..n {
            let dt_ms = (samples[i].ts_ms - samples[i - 1].ts_ms) as f64;
            if dt_ms <= 0.0 {
                continue;
            }
            
            let dx = samples[i].logit_mid - samples[i - 1].logit_mid;
            let dt_sec = dt_ms / 1000.0;
            
            // Squared change normalized by time (variance per second)
            sum_sq += dx * dx / dt_sec;
            sum_change += 1.0;
            
            // Exponential weighting: recent samples matter more
            // Weight decays with age from the end of the window
            let recency = (i as f64) / (n as f64); // 0 = oldest, 1 = newest
            let weight = recency.powi(2); // Quadratic decay toward older samples
            
            // Direction: positive dx = price going up
            let direction = if dx > 0.0 { 1.0 } else if dx < 0.0 { -1.0 } else { 0.0 };
            weighted_direction += weight * direction;
            weight_sum += weight;
        }

        let realized_vol = if sum_change > 0.0 {
            (sum_sq / sum_change).sqrt()
        } else {
            0.0
        };

        // Momentum: -1 (strong down) to +1 (strong up)
        let momentum = if weight_sum > 0.0 {
            (weighted_direction / weight_sum).clamp(-1.0, 1.0)
        } else {
            0.0
        };

        // Calmness: inverse of realized vol, normalized
        // A realized_vol of ~0.5 is "normal", below is calm, above is volatile
        const BASELINE_VOL: f64 = 0.5;
        let calmness = (BASELINE_VOL / (realized_vol + 0.01)).clamp(0.0, 2.0) / 2.0;

        VolatilityMetrics {
            realized_vol,
            momentum,
            calmness,
        }
    }

    pub fn ranges(&self) -> TrendRanges {
        TrendRanges {
            up_bid: Self::range_from(&self.min_up_bid, &self.max_up_bid),
            up_ask: Self::range_from(&self.min_up_ask, &self.max_up_ask),
            down_bid: Self::range_from(&self.min_down_bid, &self.max_down_bid),
            down_ask: Self::range_from(&self.min_down_ask, &self.max_down_ask),
        }
    }

    pub fn rn_jd_summary(&mut self) -> Option<RnJdSummary> {
        if self.samples.len() < 2 {
            return None;
        }
        let log_odds_increments = self
            .samples
            .iter()
            .zip(self.samples.iter().skip(1))
            .map(|(a, b)| b.logit_mid - a.logit_mid)
            .collect::<Vec<_>>();

        self.rn_jd_params = rn_jd::calibrate_step_em(&log_odds_increments, &self.rn_jd_params);
        let current_x = self.samples.back()?.logit_mid;
        let drift = rn_jd::calculate_rn_drift(current_x, &self.rn_jd_params, RNJD_MC_SAMPLES);
        
        // Calculate volatility metrics for adaptive spread sizing
        let vol_metrics = self.volatility_metrics();
        
        // Adapt risk aversion based on volatility: more risk averse when volatile
        let effective_gamma = RNJD_RISK_AVERSION * (1.0 + vol_metrics.realized_vol);
        
        let rnjd_quotes = rn_jd::quote_with_drift(
            current_x,
            0.0,
            effective_gamma,
            RNJD_TIME_HORIZON,
            self.rn_jd_params.sigma_b,
            RNJD_K_LIQUIDITY,
            true,
        );
        let naive_quotes = rn_jd::quote_with_drift(
            current_x,
            0.0,
            effective_gamma,
            RNJD_TIME_HORIZON,
            self.rn_jd_params.sigma_b,
            RNJD_K_LIQUIDITY,
            false,
        );

        Some(RnJdSummary {
            sigma_b: self.rn_jd_params.sigma_b,
            jump_intensity: self.rn_jd_params.jump_intensity,
            jump_std: self.rn_jd_params.jump_std,
            drift,
            rn_bid: rnjd_quotes.bid_prob,
            rn_ask: rnjd_quotes.ask_prob,
            naive_bid: naive_quotes.bid_prob,
            naive_ask: naive_quotes.ask_prob,
            reservation_log_odds: rnjd_quotes.reservation_log_odds,
            vol_metrics,
        })
    }

    fn prune(&mut self, window_ms: i64, ts_ms: i64) {
        while let Some(front) = self.samples.front() {
            if ts_ms - front.ts_ms <= window_ms {
                break;
            }
            let expired = self.samples.pop_front().unwrap();
            self.expire_min_max(
                expired.ts_ms,
                expired.up_bid,
                expired.up_ask,
                expired.down_bid,
                expired.down_ask,
            );
        }
    }

    fn push_min_max(
        &mut self,
        ts_ms: i64,
        up_bid: Decimal,
        up_ask: Decimal,
        down_bid: Decimal,
        down_ask: Decimal,
    ) {
        Self::push_min(&mut self.min_up_bid, ts_ms, up_bid);
        Self::push_max(&mut self.max_up_bid, ts_ms, up_bid);
        Self::push_min(&mut self.min_up_ask, ts_ms, up_ask);
        Self::push_max(&mut self.max_up_ask, ts_ms, up_ask);
        Self::push_min(&mut self.min_down_bid, ts_ms, down_bid);
        Self::push_max(&mut self.max_down_bid, ts_ms, down_bid);
        Self::push_min(&mut self.min_down_ask, ts_ms, down_ask);
        Self::push_max(&mut self.max_down_ask, ts_ms, down_ask);
    }

    fn expire_min_max(
        &mut self,
        ts_ms: i64,
        up_bid: Decimal,
        up_ask: Decimal,
        down_bid: Decimal,
        down_ask: Decimal,
    ) {
        Self::expire(&mut self.min_up_bid, ts_ms, up_bid);
        Self::expire(&mut self.max_up_bid, ts_ms, up_bid);
        Self::expire(&mut self.min_up_ask, ts_ms, up_ask);
        Self::expire(&mut self.max_up_ask, ts_ms, up_ask);
        Self::expire(&mut self.min_down_bid, ts_ms, down_bid);
        Self::expire(&mut self.max_down_bid, ts_ms, down_bid);
        Self::expire(&mut self.min_down_ask, ts_ms, down_ask);
        Self::expire(&mut self.max_down_ask, ts_ms, down_ask);
    }

    fn push_min(deque: &mut VecDeque<WindowPoint>, ts_ms: i64, value: Decimal) {
        while let Some(back) = deque.back() {
            if back.value <= value {
                break;
            }
            deque.pop_back();
        }
        deque.push_back(WindowPoint { ts_ms, value });
    }

    fn push_max(deque: &mut VecDeque<WindowPoint>, ts_ms: i64, value: Decimal) {
        while let Some(back) = deque.back() {
            if back.value >= value {
                break;
            }
            deque.pop_back();
        }
        deque.push_back(WindowPoint { ts_ms, value });
    }

    fn expire(deque: &mut VecDeque<WindowPoint>, ts_ms: i64, value: Decimal) {
        if let Some(front) = deque.front() {
            if front.ts_ms == ts_ms && front.value == value {
                deque.pop_front();
            }
        }
    }

    fn range_from(min: &VecDeque<WindowPoint>, max: &VecDeque<WindowPoint>) -> Decimal {
        match (min.front(), max.front()) {
            (Some(min), Some(max)) => max.value - min.value,
            _ => Decimal::ZERO,
        }
    }
}

/// Volatility metrics for adaptive spread sizing
#[derive(Debug, Clone, Copy, Default)]
pub struct VolatilityMetrics {
    /// Realized volatility: sqrt of mean squared log-odds changes per second
    pub realized_vol: f64,
    /// Momentum: exponentially-weighted recent price direction (-1 to +1)
    pub momentum: f64,
    /// How "calm" the market is (0 = volatile, 1 = very calm)
    pub calmness: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct RnJdSummary {
    pub sigma_b: f64,
    pub jump_intensity: f64,
    pub jump_std: f64,
    pub drift: f64,
    pub rn_bid: f64,
    pub rn_ask: f64,
    pub naive_bid: f64,
    pub naive_ask: f64,
    pub reservation_log_odds: f64,
    /// Volatility metrics for adaptive spread sizing
    pub vol_metrics: VolatilityMetrics,
}

#[derive(Debug, Clone, Copy)]
pub struct TrendRanges {
    pub up_bid: Decimal,
    pub up_ask: Decimal,
    pub down_bid: Decimal,
    pub down_ask: Decimal,
}

impl TrendRanges {
    pub fn max(&self) -> Decimal {
        self.up_bid
            .max(self.up_ask)
            .max(self.down_bid)
            .max(self.down_ask)
    }
}

fn decimal_mid_prob(bid: Decimal, ask: Decimal) -> f64 {
    let mid = (bid + ask) / Decimal::from(2u32);
    mid.to_f64().unwrap_or(0.5)
}

impl Default for TrendWindow {
    fn default() -> Self {
        Self {
            samples: VecDeque::new(),
            first_seen_ms: None,
            last_seen_ms: None,
            min_up_bid: VecDeque::new(),
            max_up_bid: VecDeque::new(),
            min_up_ask: VecDeque::new(),
            max_up_ask: VecDeque::new(),
            min_down_bid: VecDeque::new(),
            max_down_bid: VecDeque::new(),
            min_down_ask: VecDeque::new(),
            max_down_ask: VecDeque::new(),
            rn_jd_params: MarketParams {
                sigma_b: 0.5,
                jump_intensity: 0.1,
                jump_std: 1.0,
            },
            first_mid: None,
            last_mid: None,
        }
    }
}
