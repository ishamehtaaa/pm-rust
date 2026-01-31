use rand_distr::{Distribution, Normal};

const DT: f64 = 1.0;
const TRUNCATION_CUTOFF: f64 = 1.0;
const EPSILON: f64 = 1e-5;

/// Smoothing factor for parameter estimates (0 = no smoothing, 1 = full smoothing)
/// 0.3 means 30% weight on new estimate, 70% on previous
const PARAM_SMOOTHING_ALPHA: f64 = 0.3;

#[derive(Debug, Clone, Copy)]
pub struct MarketParams {
    pub sigma_b: f64,
    pub jump_intensity: f64,
    pub jump_std: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct Quote {
    pub bid_prob: f64,
    pub ask_prob: f64,
    pub reservation_log_odds: f64,
}

pub fn logit(p: f64) -> f64 {
    let p_clamped = p.clamp(EPSILON, 1.0 - EPSILON);
    (p_clamped / (1.0 - p_clamped)).ln()
}

pub fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

fn s_prime(x: f64) -> f64 {
    let p = sigmoid(x);
    p * (1.0 - p)
}

fn s_double_prime(x: f64) -> f64 {
    let p = sigmoid(x);
    p * (1.0 - p) * (1.0 - 2.0 * p)
}

pub fn calculate_rn_drift(x: f64, params: &MarketParams, mc_samples: usize) -> f64 {
    let sp = s_prime(x);
    let convexity_term = -0.5 * s_double_prime(x) * params.sigma_b.powi(2) / sp;

    let mut rng = rand::thread_rng();
    let jump_dist = Normal::new(0.0, params.jump_std).unwrap();
    let mut jump_integral_sum = 0.0;

    for _ in 0..mc_samples {
        let z = jump_dist.sample(&mut rng);
        let chi_z = if z.abs() < TRUNCATION_CUTOFF { z } else { 0.0 };
        let jump_impact = sigmoid(x + z) - sigmoid(x) - sp * chi_z;
        jump_integral_sum += jump_impact;
    }

    let expected_jump_impact = jump_integral_sum / (mc_samples as f64);
    let jump_term = params.jump_intensity * expected_jump_impact / sp;

    convexity_term + jump_term
}

pub fn drift_correction(x: f64, sigma_b: f64) -> f64 {
    let sp = s_prime(x);
    if sp < 1e-5 {
        return 0.0;
    }
    -0.5 * sigma_b.powi(2) * s_double_prime(x) / sp
}

pub fn quote_with_drift(
    current_x: f64,
    inventory_q: f64,
    risk_aversion_gamma: f64,
    time_horizon_t_minus_t: f64,
    sigma_b: f64,
    k_liquidity_param: f64,
    use_drift: bool,
) -> Quote {
    let drift = if use_drift {
        drift_correction(current_x, sigma_b) * DT
    } else {
        0.0
    };
    let fair_x = current_x + drift;
    let reservation_x =
        fair_x - (inventory_q * risk_aversion_gamma * sigma_b.powi(2) * time_horizon_t_minus_t);

    let vol_component = risk_aversion_gamma * sigma_b.powi(2) * time_horizon_t_minus_t;
    let liquidity_component =
        (2.0 / k_liquidity_param) * (1.0 + risk_aversion_gamma / k_liquidity_param).ln();
    let total_spread_x = vol_component + liquidity_component;
    let half_spread_x = total_spread_x / 2.0;

    let bid_x = reservation_x - half_spread_x;
    let ask_x = reservation_x + half_spread_x;

    Quote {
        bid_prob: sigmoid(bid_x),
        ask_prob: sigmoid(ask_x),
        reservation_log_odds: reservation_x,
    }
}

/// Calibrate jump-diffusion parameters using EM algorithm with exponential smoothing.
/// Smoothing reduces noise from short windows, giving more stable fair value estimates.
pub fn calibrate_step_em(
    log_odds_increments: &[f64],
    current_params: &MarketParams,
) -> MarketParams {
    let mut new_sigma_sq_sum = 0.0;
    let mut new_lambda_sum = 0.0;
    let mut weights_sum = 0.0;
    let drift_approx = 0.0;

    for &dx in log_odds_increments {
        let diff_sd = current_params.sigma_b * DT.sqrt();
        let phi = pdf_normal(dx, drift_approx, diff_sd);

        let jump_sd = (current_params.jump_std.powi(2) + diff_sd.powi(2)).sqrt();
        let psi = pdf_normal(dx, drift_approx, jump_sd);

        let p_jump = current_params.jump_intensity * DT;
        let numerator = p_jump * psi;
        let denominator = numerator + (1.0 - p_jump) * phi;
        let gamma = if denominator > 0.0 {
            numerator / denominator
        } else {
            0.0
        };

        new_sigma_sq_sum += (1.0 - gamma) * dx.powi(2);
        weights_sum += 1.0 - gamma;
        new_lambda_sum += gamma;
    }

    let n = log_odds_increments.len() as f64;

    // Raw estimates from this window
    let raw_sigma_b = if weights_sum > 0.0 {
        (new_sigma_sq_sum / (weights_sum * DT)).sqrt()
    } else {
        current_params.sigma_b
    };
    let raw_jump_intensity = if n > 0.0 {
        (new_lambda_sum / n) / DT
    } else {
        0.0
    };

    // Apply exponential smoothing: blend new estimate with previous
    // This reduces noise from short 8-second windows
    let smoothed_sigma_b = PARAM_SMOOTHING_ALPHA * raw_sigma_b
        + (1.0 - PARAM_SMOOTHING_ALPHA) * current_params.sigma_b;

    let smoothed_jump_intensity = PARAM_SMOOTHING_ALPHA * raw_jump_intensity
        + (1.0 - PARAM_SMOOTHING_ALPHA) * current_params.jump_intensity;

    // Clamp to reasonable bounds to prevent extreme estimates
    let sigma_b = smoothed_sigma_b.clamp(0.1, 2.0);
    let jump_intensity = smoothed_jump_intensity.clamp(0.0, 0.5);

    MarketParams {
        sigma_b,
        jump_intensity,
        jump_std: current_params.jump_std,
    }
}

fn pdf_normal(x: f64, mean: f64, std: f64) -> f64 {
    let exponent = -0.5 * ((x - mean) / std).powi(2);
    (1.0 / (std * (2.0 * std::f64::consts::PI).sqrt())) * exponent.exp()
}

pub fn generate_quotes(
    current_x: f64,
    inventory_q: f64,
    risk_aversion_gamma: f64,
    time_horizon_t_minus_t: f64,
    params: &MarketParams,
    k_liquidity_param: f64,
) -> Quote {
    let reservation_x = current_x
        - (inventory_q * risk_aversion_gamma * params.sigma_b.powi(2) * time_horizon_t_minus_t);

    let vol_component = risk_aversion_gamma * params.sigma_b.powi(2) * time_horizon_t_minus_t;
    let liquidity_component =
        (2.0 / k_liquidity_param) * (1.0 + risk_aversion_gamma / k_liquidity_param).ln();
    let total_spread_x = vol_component + liquidity_component;
    let half_spread_x = total_spread_x / 2.0;

    let bid_x = reservation_x - half_spread_x;
    let ask_x = reservation_x + half_spread_x;

    Quote {
        bid_prob: sigmoid(bid_x),
        ask_prob: sigmoid(ask_x),
        reservation_log_odds: reservation_x,
    }
}
