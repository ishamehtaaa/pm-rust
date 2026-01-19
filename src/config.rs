use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";
const DEFAULT_SHARES_TARGET_PER_SIDE: Decimal = dec!(15);
const DEFAULT_ORDER_SIZE: Decimal = dec!(5);
const DEFAULT_TARGET_TOTAL_COST: Decimal = dec!(0.96); /* Tighter to ensure profitability with execution slippage */
const DEFAULT_MAKER_PRICE_OFFSET: Decimal = dec!(0.01);
const DEFAULT_MAX_PRICE_AGE_MS: i64 = 2_500;
const DEFAULT_COOLDOWN_SECS: u64 = 0;
const DEFAULT_TREND_WINDOW_SECS: u64 = 12;
const DEFAULT_SIZE_SCALE_MIN: Decimal = dec!(0.5);
const DEFAULT_SIZE_SCALE_MAX: Decimal = dec!(2.0);
const DEFAULT_DURATION: MarketDuration = MarketDuration::FifteenMin;
const DEFAULT_ALLOW_EXTREME_SKEW: bool = false;

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
    pub size_scale_min: Decimal,
    pub size_scale_max: Decimal,
    pub target_duration: MarketDuration,
    pub allow_extreme_skew: bool,
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
            size_scale_min: DEFAULT_SIZE_SCALE_MIN,
            size_scale_max: DEFAULT_SIZE_SCALE_MAX,
            target_duration: DEFAULT_DURATION,
            allow_extreme_skew: DEFAULT_ALLOW_EXTREME_SKEW,
            dry_run: false,
        })
    }
}
