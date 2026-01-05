use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";

pub static PAUSED_ASSETS: Lazy<HashSet<&'static str>> = Lazy::new(|| HashSet::from(["ethereum"]));

#[derive(Debug, Clone)]
pub struct AssetInfo {
    /* this is the name of the asset (btc, eth) we are trading */
    pub asset: String,
    pub prefixes: Vec<String>,
    pub chainlink: String,
    pub binance: String,
}

pub static ASSET_CONFIG: Lazy<Vec<(&str, &[&str], &str, &str)>> = Lazy::new(|| {
    vec![
        ("bitcoin", &["btc", "bitcoin"], "btc/usd", "btcusdt"),
        ("ethereum", &["eth", "ethereum"], "eth/usd", "ethusdt"),
        ("solana", &["sol", "solana"], "sol/usd", "solusdt"),
        ("xrp", &["xrp", "xrp"], "xrp/usd", "xrpusdt"),
    ]
});

pub static ASSETS_BY_NAME: Lazy<HashMap<String, AssetInfo>> = Lazy::new(|| {
    ASSET_CONFIG
        .iter()
        .map(|(asset, prefixes, chainlink, binance)| {
            (
                asset.to_string(),
                AssetInfo {
                    asset: asset.to_string(),
                    prefixes: prefixes.iter().map(|s| s.to_string()).collect(),
                    chainlink: chainlink.to_string(),
                    binance: binance.to_string(),
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

pub static TARGET_ASSETS: Lazy<HashSet<String>> = Lazy::new(|| {
    HashSet::from([
        "bitcoin".to_string(),
        // "solana".to_string(),
        // "ethereum".to_string(),
        // "xrp".to_string(),
    ])
});

#[derive(Debug, Clone)]
pub struct LeggingConfig {
    /// Target combined price for Up + Down (e.g., 0.98 = 2% profit)
    pub target_combined: Decimal,

    /// Buffer added to ask when completing second leg as taker
    pub taker_buffer: Decimal,

    /// How much the target bid must change before we cancel and repost
    pub requote_threshold: Decimal,

    /// Size per resting order
    pub shares_per_trade: Decimal,

    /// Max exposure per market before pausing new quotes
    pub target_shares_per_market: Decimal,
}

/// Runtime settings derived from user config/environment.
#[derive(Debug, Clone)]
pub struct BotSettings {
    pub status_interval: Duration,
    pub ladder_interval: Duration,
    pub rollover_interval: Duration,
    pub cleanup_interval: Duration,
    pub positions_poll_interval: Duration,
    pub min_price: Decimal,
    pub max_price: Decimal,
    pub price_tick: Decimal,
    pub min_order_size: Decimal,
    pub min_notional: Decimal,
    pub imbalance_threshold: Decimal,
    pub levels_per_side: usize,
    pub taker_cooldown: Duration,
    pub balance_error_cooldown: Duration,
}

impl Default for BotSettings {
    fn default() -> Self {
        Self {
            status_interval: Duration::from_secs(10),
            ladder_interval: Duration::from_secs(2),
            rollover_interval: Duration::from_secs(1),
            cleanup_interval: Duration::from_secs(300),
            positions_poll_interval: Duration::from_secs(30),
            min_price: dec!(0.01),
            max_price: dec!(0.99),
            price_tick: dec!(0.01),
            min_order_size: dec!(5.0),
            min_notional: dec!(1.0),
            imbalance_threshold: dec!(2.0),
            levels_per_side: 5,
            taker_cooldown: Duration::from_secs(2),
            balance_error_cooldown: Duration::from_secs(30),
        }
    }
}

impl Default for LeggingConfig {
    fn default() -> Self {
        Self {
            // If down_ask = 0.82, we bid up @ 0.16 (combined = 0.98)
            target_combined: dec!(0.97),

            // When completing second leg, add this to the ask
            taker_buffer: dec!(0.01),

            // Only requote if price moved more than this
            requote_threshold: dec!(0.01),

            // Size per order
            shares_per_trade: dec!(8.0),

            // Stop posting new quotes after this exposure
            target_shares_per_market: dec!(32.0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dry_run: bool,
    pub polymarket_private_key: String,
    pub polymarket_proxy_address: String,
    pub target_assets: HashSet<String>,
    pub legging_config: LeggingConfig,
    pub bot_settings: BotSettings,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let dry_run = false;

        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address =
            std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();

        let legging_config = LeggingConfig::default();

        Ok(Self {
            dry_run,
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets: TARGET_ASSETS.clone(),
            legging_config,
            bot_settings: BotSettings::default(),
        })
    }
}
