use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::VecDeque;

use crate::rn_jd::{self, MarketParams};

const RNJD_MC_SAMPLES: usize = 200;
const KALMAN_MEASUREMENT_VAR: f64 = 0.0004;
const KALMAN_INIT_VAR: f64 = 0.1;

#[derive(Debug)]
pub struct TrendWindow {
    samples: VecDeque<TrendSample>,
    first_seen_ms: Option<i64>,
    last_seen_ms: Option<i64>,
    last_ts_ms: Option<i64>,
    kalman_x: Option<f64>,
    kalman_p: f64,
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
    filtered_x: f64,
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
        let filtered_x = self.update_kalman(ts_ms, logit_mid);
        if self.first_seen_ms.is_none() {
            self.first_seen_ms = Some(ts_ms);
            self.first_mid = Some(mid_prob);
        }
        self.last_seen_ms = Some(ts_ms);
        self.last_ts_ms = Some(ts_ms);
        self.last_mid = Some(mid_prob);
        self.samples.push_back(TrendSample {
            ts_ms,
            up_bid,
            up_ask,
            down_bid,
            down_ask,
            logit_mid,
            filtered_x,
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
            .map(|(a, b)| b.filtered_x - a.filtered_x)
            .collect::<Vec<_>>();

        self.rn_jd_params = rn_jd::calibrate_step_em(&log_odds_increments, &self.rn_jd_params);
        let current_x = self.kalman_x.unwrap_or(self.samples.back()?.logit_mid);
        let drift = rn_jd::calculate_rn_drift(current_x, &self.rn_jd_params, RNJD_MC_SAMPLES);

        Some(RnJdSummary {
            sigma_b: self.rn_jd_params.sigma_b,
            jump_intensity: self.rn_jd_params.jump_intensity,
            jump_std: self.rn_jd_params.jump_std,
            drift,
            filtered_x: current_x,
        })
    }

    pub fn order_imbalance(&self, window_secs: u64) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }
        let window_ms = (window_secs as i64) * 1000;
        let mut up_moves = 0;
        let mut down_moves = 0;
        let mut last = None;

        for sample in self.samples.iter().rev() {
            if let Some(last_ts) = self.last_seen_ms {
                if last_ts - sample.ts_ms > window_ms {
                    break;
                }
            }
            if let Some(prev) = last {
                let delta = prev - sample.filtered_x;
                if delta > 0.0 {
                    up_moves += 1;
                } else if delta < 0.0 {
                    down_moves += 1;
                }
            }
            last = Some(sample.filtered_x);
        }

        let total = up_moves + down_moves;
        if total == 0 {
            return 0.0;
        }
        ((up_moves as f64 - down_moves as f64).abs()) / (total as f64)
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

#[derive(Debug, Clone, Copy)]
pub struct RnJdSummary {
    pub sigma_b: f64,
    pub jump_intensity: f64,
    pub jump_std: f64,
    pub drift: f64,
    pub filtered_x: f64,
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
            last_ts_ms: None,
            kalman_x: None,
            kalman_p: KALMAN_INIT_VAR,
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

impl TrendWindow {
    fn update_kalman(&mut self, ts_ms: i64, measurement: f64) -> f64 {
        let prev_x = self.kalman_x.unwrap_or(measurement);
        let dt_secs = match self.last_ts_ms {
            Some(last) => ((ts_ms - last) as f64 / 1000.0).max(0.001),
            None => 1.0,
        };

        let drift = rn_jd::calculate_rn_drift(prev_x, &self.rn_jd_params, RNJD_MC_SAMPLES);
        let predict_x = prev_x + drift * dt_secs;
        let process_var = self.rn_jd_params.sigma_b.powi(2) * dt_secs;
        let mut p = self.kalman_p + process_var;

        let k = p / (p + KALMAN_MEASUREMENT_VAR);
        let x = predict_x + k * (measurement - predict_x);
        p = (1.0 - k) * p;

        self.kalman_x = Some(x);
        self.kalman_p = p;
        x
    }
}
