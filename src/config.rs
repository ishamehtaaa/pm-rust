use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";
const DEFAULT_SHARES_TARGET_PER_SIDE: Decimal = dec!(20);
const DEFAULT_ORDER_SIZE: Decimal = dec!(5);
const DEFAULT_TARGET_TOTAL_COST: Decimal = dec!(0.98);
const DEFAULT_MAKER_PRICE_OFFSET: Decimal = dec!(0.01);
const DEFAULT_MAX_PRICE_AGE_MS: i64 = 2_500;
const DEFAULT_COOLDOWN_SECS: u64 = 2;
const DEFAULT_TREND_WINDOW_SECS: u64 = 8;
const DEFAULT_TREND_MAX_RANGE: Decimal = dec!(0.10);
const DEFAULT_MAX_SIDE_SPREAD: Decimal = dec!(0.05);
const DEFAULT_SWING_ZONE_LOW: Decimal = dec!(0.35);
const DEFAULT_SWING_ZONE_HIGH: Decimal = dec!(0.65);
const DEFAULT_SWING_ZONE_TARGET_FACTOR: Decimal = dec!(0.6);
const DEFAULT_NEWS_GUARD_WINDOW_SECS: u64 = 300;
const DEFAULT_NEWS_EVENT_TIMES: &[&str] = &[];
const DEFAULT_DIRECTIONAL_MOVE_THRESHOLD: Decimal = dec!(0.05);
const DEFAULT_HIGH_VOL_REVERSION_THRESHOLD: Decimal = dec!(0.02);
const DEFAULT_MIN_LIQUIDITY_SCALE: Decimal = dec!(0.2);
const DEFAULT_LAMBDA_PAUSE_THRESHOLD: f64 = 0.25;
const DEFAULT_TICK_SIZE: Decimal = dec!(0.01);
const DEFAULT_EDGE_THRESHOLD: Decimal = dec!(0.0025);
const DEFAULT_SIZE_SCALE_MIN: Decimal = dec!(0.5);
const DEFAULT_SIZE_SCALE_MAX: Decimal = dec!(2.0);
const DEFAULT_WIDEN_FACTOR: Decimal = dec!(2.0);
const DEFAULT_DRIFT_FLICKER_THRESHOLD: f64 = 0.0005;
const DEFAULT_PINNED_LOW: Decimal = dec!(0.05);
const DEFAULT_PINNED_HIGH: Decimal = dec!(0.95);
const DEFAULT_DURATION: MarketDuration = MarketDuration::FifteenMin;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDuration {
    FifteenMin,
    OneHour,
}

impl MarketDuration {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarketDuration::FifteenMin => "15m",
            MarketDuration::OneHour => "1h",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AssetInfo {
    pub asset: String,
    pub prefixes: Vec<String>,
}

pub static ASSET_CONFIG: Lazy<Vec<(&str, &[&str])>> = Lazy::new(|| {
    vec![
        ("bitcoin", &["btc", "bitcoin"]),
        ("ethereum", &["eth", "ethereum"]),
        ("solana", &["sol", "solana"]),
        ("xrp", &["xrp", "xrp"]),
    ]
});

pub static ASSETS_BY_NAME: Lazy<HashMap<String, AssetInfo>> = Lazy::new(|| {
    ASSET_CONFIG
        .iter()
        .map(|(asset, prefixes)| {
            (
                asset.to_string(),
                AssetInfo {
                    asset: asset.to_string(),
                    prefixes: prefixes.iter().map(|s| s.to_string()).collect(),
                },
            )
        })
        .collect()
});

pub static ASSETS_BY_PREFIX: Lazy<HashMap<String, AssetInfo>> = Lazy::new(|| {
    let mut map = HashMap::new();
    for info in ASSETS_BY_NAME.values() {
        for prefix in &info.prefixes {
            map.insert(prefix.clone(), info.clone());
        }
    }
    map
});

pub static TARGET_ASSETS: Lazy<HashSet<String>> =
    Lazy::new(|| HashSet::from(["bitcoin".to_string()]));

#[derive(Debug, Clone)]
pub struct Config {
    pub dry_run: bool,
    pub polymarket_private_key: String,
    pub polymarket_proxy_address: String,
    pub target_assets: HashSet<String>,
    pub shares_target_per_side: Decimal,
    pub order_size: Decimal,
    pub target_total_cost: Decimal,
    pub maker_price_offset: Decimal,
    pub max_price_age_ms: i64,
    pub cooldown_secs: u64,
    pub trend_window_secs: u64,
    pub trend_max_range: Decimal,
    pub max_side_spread: Decimal,
    pub swing_zone_low: Decimal,
    pub swing_zone_high: Decimal,
    pub swing_zone_target_factor: Decimal,
    pub news_guard_window_secs: u64,
    pub news_event_times: Vec<chrono::DateTime<chrono::Utc>>,
    pub directional_move_threshold: Decimal,
    pub high_vol_reversion_threshold: Decimal,
    pub min_liquidity_scale: Decimal,
    pub lambda_pause_threshold: f64,
    pub tick_size: Decimal,
    pub edge_threshold: Decimal,
    pub size_scale_min: Decimal,
    pub size_scale_max: Decimal,
    pub widen_factor: Decimal,
    pub drift_flicker_threshold: f64,
    pub pinned_low: Decimal,
    pub pinned_high: Decimal,
    pub target_duration: MarketDuration,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address =
            std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();

        let target_assets = TARGET_ASSETS.clone();

        Ok(Self {
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets,
            shares_target_per_side: DEFAULT_SHARES_TARGET_PER_SIDE,
            order_size: DEFAULT_ORDER_SIZE,
            target_total_cost: DEFAULT_TARGET_TOTAL_COST,
            maker_price_offset: DEFAULT_MAKER_PRICE_OFFSET,
            max_price_age_ms: DEFAULT_MAX_PRICE_AGE_MS,
            cooldown_secs: DEFAULT_COOLDOWN_SECS,
            trend_window_secs: DEFAULT_TREND_WINDOW_SECS,
            trend_max_range: DEFAULT_TREND_MAX_RANGE,
            max_side_spread: DEFAULT_MAX_SIDE_SPREAD,
            swing_zone_low: DEFAULT_SWING_ZONE_LOW,
            swing_zone_high: DEFAULT_SWING_ZONE_HIGH,
            swing_zone_target_factor: DEFAULT_SWING_ZONE_TARGET_FACTOR,
            news_guard_window_secs: DEFAULT_NEWS_GUARD_WINDOW_SECS,
            news_event_times: DEFAULT_NEWS_EVENT_TIMES
                .iter()
                .map(|s| chrono::DateTime::parse_from_rfc3339(s))
                .map(|r| r.map(|dt| dt.with_timezone(&chrono::Utc)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| anyhow::anyhow!("Invalid news event time: {}", e))?,
            directional_move_threshold: DEFAULT_DIRECTIONAL_MOVE_THRESHOLD,
            high_vol_reversion_threshold: DEFAULT_HIGH_VOL_REVERSION_THRESHOLD,
            min_liquidity_scale: DEFAULT_MIN_LIQUIDITY_SCALE,
            lambda_pause_threshold: DEFAULT_LAMBDA_PAUSE_THRESHOLD,
            tick_size: DEFAULT_TICK_SIZE,
            edge_threshold: DEFAULT_EDGE_THRESHOLD,
            size_scale_min: DEFAULT_SIZE_SCALE_MIN,
            size_scale_max: DEFAULT_SIZE_SCALE_MAX,
            widen_factor: DEFAULT_WIDEN_FACTOR,
            drift_flicker_threshold: DEFAULT_DRIFT_FLICKER_THRESHOLD,
            pinned_low: DEFAULT_PINNED_LOW,
            pinned_high: DEFAULT_PINNED_HIGH,
            target_duration: DEFAULT_DURATION,
            dry_run: false,
        })
    }
}
