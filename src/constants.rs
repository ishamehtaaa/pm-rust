use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub const SHARE_DECIMALS: Decimal = dec!(1_000_000);
pub const SIZE_DP: u32 = 2;

/* Convert raw sizes to human readable shares. */
pub fn to_shares(raw: Decimal) -> Decimal {
    (raw / SHARE_DECIMALS).round_dp(SIZE_DP)
}

/* Convert human readable share amounts to raw sizes. */
pub fn to_raw(shares: Decimal) -> Decimal {
    shares * SHARE_DECIMALS
}

/* Round a share amount to a valid lot size. */
pub fn round_size(size: Decimal) -> Decimal {
    size.round_dp_with_strategy(SIZE_DP, rust_decimal::RoundingStrategy::ToZero)
}

/* Used to truncate long order ids for logging. */
pub fn short_id(s: &str, n: usize) -> &str {
    &s[..s.len().min(n)]
}
