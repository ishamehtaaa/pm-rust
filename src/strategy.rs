use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Trading strategy parameters.
///
/// These are **not** environment/runtime config (keys, assets, etc). They're the
/// internal tuning knobs for the current strategy implementation.
#[derive(Debug, Clone)]
pub struct StrategyParams {
    pub order_size: Decimal,
    pub target_total_cost: Decimal, // Up+Down max combined cost
    pub maker_price_offset: Decimal,
    pub max_price_age_ms: i64,
    pub cooldown_secs: u64,
    pub trend_window_secs: u64,
    pub size_scale_min: Decimal,
    pub size_scale_max: Decimal,
}

impl Default for StrategyParams {
    fn default() -> Self {
        Self {
            order_size: dec!(5),
            // 0.98 => ~2¢/share gross edge before fees
            target_total_cost: dec!(0.98),
            maker_price_offset: dec!(0.01),
            max_price_age_ms: 2_500,
            cooldown_secs: 2,
            // Longer window = more stable signal, fewer whipsaws.
            trend_window_secs: 30,
            size_scale_min: dec!(0.5),
            size_scale_max: dec!(2.0),
        }
    }
}
