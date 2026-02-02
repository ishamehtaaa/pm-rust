use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";

// ============================================================================
// Core Trading Parameters (the ones that matter for continuous trading)
// ============================================================================

/// Target shares to accumulate per side (Up and Down) for each market
const DEFAULT_TARGET_PER_SIDE: Decimal = dec!(5000);

/// Size of each individual order (like the target trader's ~24 share orders)
const DEFAULT_ORDER_SIZE: Decimal = dec!(12);

/// Maximum combined cost (up_avg + down_avg) to accept
/// 0.995 = require at least 0.5% edge on aggregate position
const DEFAULT_MAX_COMBINED_COST: Decimal = dec!(0.990);

/// Maximum imbalance (up_shares - down_shares) before prioritizing the lighter side
/// Lower = more aggressive rebalancing
const DEFAULT_MAX_IMBALANCE: Decimal = dec!(50);

/// Number of resting order levels per side
const DEFAULT_LEVELS: usize = 5;

/// Price offset below best ask for order placement
/// 0.01 = place one tick below ask (top of book, best bid)
const DEFAULT_PRICE_OFFSET: Decimal = dec!(0.01);

/// Spacing between ladder levels  
/// Tighter spacing = more orders near the ask
const DEFAULT_LEVEL_SPACING: Decimal = dec!(0.01);

// ============================================================================
// Safety Parameters
// ============================================================================

/// Maximum age of price data before considering it stale (ms)
const DEFAULT_MAX_PRICE_AGE_MS: i64 = 1_000;

/// Minimum time between order placements per market (seconds)
const DEFAULT_COOLDOWN_SECS: u64 = 0;

/// Price movement threshold to trigger re-laddering
/// Lower = reladder more often (add new orders frequently)
const DEFAULT_RELADDER_THRESHOLD: Decimal = dec!(0.01);

/// Distance from current ask to consider an order stale
/// Very high = almost never cancel orders, let them sit
const DEFAULT_STALE_ORDER_DISTANCE: Decimal = dec!(0.50);

/// Combined cost threshold to trigger aggressive taker mode
/// When up_ask + down_ask < this value, take liquidity immediately
/// 0.99 = trigger taker mode when there's 1%+ edge
const DEFAULT_TAKER_THRESHOLD: Decimal = dec!(0.99);

/// Size multiplier for taker orders
/// 2x base size = 48 shares per order (balanced accumulation)
const DEFAULT_TAKER_SIZE_MULTIPLIER: Decimal = dec!(2.0);

/// Tick size for price rounding
const DEFAULT_TICK_SIZE: Decimal = dec!(0.01);

// ============================================================================
// Market Duration
// ============================================================================

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

// ============================================================================
// Asset Configuration
// ============================================================================

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

// ============================================================================
// Trading Configuration
// ============================================================================

#[derive(Debug, Clone)]
pub struct TradingConfig {
    /// Target shares per side
    pub target_per_side: Decimal,
    /// Order size per placement
    pub order_size: Decimal,
    /// Maximum combined cost (edge threshold)
    pub max_combined_cost: Decimal,
    /// Maximum imbalance before prioritizing lighter side
    pub max_imbalance: Decimal,
    /// Number of ladder levels
    pub levels: usize,
    /// Price offset below ask
    pub price_offset: Decimal,
    /// Spacing between levels
    pub level_spacing: Decimal,
    /// Reladder threshold (price movement to trigger refresh)
    pub reladder_threshold: Decimal,
    /// Stale order distance
    pub stale_order_distance: Decimal,
    /// Combined cost threshold to trigger aggressive taker mode
    pub taker_threshold: Decimal,
    /// Size multiplier for taker orders
    pub taker_size_multiplier: Decimal,
}

impl Default for TradingConfig {
    fn default() -> Self {
        Self {
            target_per_side: DEFAULT_TARGET_PER_SIDE,
            order_size: DEFAULT_ORDER_SIZE,
            max_combined_cost: DEFAULT_MAX_COMBINED_COST,
            max_imbalance: DEFAULT_MAX_IMBALANCE,
            levels: DEFAULT_LEVELS,
            price_offset: DEFAULT_PRICE_OFFSET,
            level_spacing: DEFAULT_LEVEL_SPACING,
            reladder_threshold: DEFAULT_RELADDER_THRESHOLD,
            stale_order_distance: DEFAULT_STALE_ORDER_DISTANCE,
            taker_threshold: DEFAULT_TAKER_THRESHOLD,
            taker_size_multiplier: DEFAULT_TAKER_SIZE_MULTIPLIER,
        }
    }
}

// ============================================================================
// Main Config
// ============================================================================

#[derive(Debug, Clone)]
pub struct Config {
    // Authentication
    pub polymarket_private_key: String,
    pub polymarket_proxy_address: String,

    // Mode
    pub dry_run: bool,

    // Market selection
    pub target_assets: HashSet<String>,
    pub target_duration: MarketDuration,

    // Trading parameters
    pub trading: TradingConfig,

    // Safety parameters
    pub max_price_age_ms: i64,
    pub cooldown_secs: u64,
    pub tick_size: Decimal,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address =
            std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();

        Ok(Self {
            polymarket_private_key,
            polymarket_proxy_address,
            dry_run: false,
            target_assets: HashSet::from(["bitcoin".to_string()]),
            target_duration: MarketDuration::FifteenMin,
            trading: TradingConfig::default(),
            max_price_age_ms: DEFAULT_MAX_PRICE_AGE_MS,
            cooldown_secs: DEFAULT_COOLDOWN_SECS,
            tick_size: DEFAULT_TICK_SIZE,
        })
    }
}

// ============================================================================
// Legacy compatibility (for bot.rs that still uses old field names)
// These will be removed once bot.rs is updated
// ============================================================================

#[derive(Debug, Clone)]
pub struct LadderTuning {
    pub shares_target_per_side: Decimal,
    pub order_size: Decimal,
    pub target_total_cost: Decimal,
    pub maker_price_offset: Decimal,
    pub levels: usize,
    pub reladder_threshold: Decimal,
    pub stale_order_distance: Decimal,
    pub max_imbalance_shares: Decimal,
    pub allow_single_side: bool,
    pub aggressive_rebalance_threshold: Decimal,
    pub taker_threshold: Decimal,
    pub taker_size_multiplier: Decimal,
}

impl From<&TradingConfig> for LadderTuning {
    fn from(tc: &TradingConfig) -> Self {
        Self {
            shares_target_per_side: tc.target_per_side,
            order_size: tc.order_size,
            target_total_cost: tc.max_combined_cost,
            maker_price_offset: tc.price_offset,
            levels: tc.levels,
            reladder_threshold: tc.reladder_threshold,
            stale_order_distance: tc.stale_order_distance,
            max_imbalance_shares: tc.max_imbalance,
            allow_single_side: true,
            aggressive_rebalance_threshold: tc.max_imbalance * dec!(2.5),
            taker_threshold: tc.taker_threshold,
            taker_size_multiplier: tc.taker_size_multiplier,
        }
    }
}

impl Config {
    /// Legacy accessor for bot.rs compatibility
    pub fn ladder(&self) -> LadderTuning {
        LadderTuning::from(&self.trading)
    }
}
