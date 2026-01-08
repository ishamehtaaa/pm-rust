use rust_decimal::Decimal;
use rust_decimal::RoundingStrategy;

pub fn round_down_2dp(value: Decimal) -> Decimal {
    value.round_dp_with_strategy(2, RoundingStrategy::ToZero)
}

pub fn round_up_2dp(value: Decimal) -> Decimal {
    value.round_dp_with_strategy(2, RoundingStrategy::AwayFromZero)
}

pub fn round_down_for_tick(value: Decimal, tick_size: Decimal) -> Decimal {
    let tick_scale = tick_size.scale();
    let scale = std::cmp::min(2, tick_scale);
    value.round_dp_with_strategy(scale, RoundingStrategy::ToZero)
}

pub fn clamp_decimal(value: Decimal, min: Decimal, max: Decimal) -> Decimal {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}
