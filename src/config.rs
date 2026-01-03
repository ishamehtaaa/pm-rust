use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";

pub static PAUSED_ASSETS: Lazy<HashSet<&'static str>> = Lazy::new(|| HashSet::from(["ethereum"]));

#[derive(Debug, Clone)]
pub struct AssetInfo {
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
        "ethereum".to_string(),
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

impl Default for LeggingConfig {
    fn default() -> Self {
        Self {
            // If down_ask = 0.82, we bid up @ 0.16 (combined = 0.98)
            target_combined: dec!(0.98),

            // When completing second leg, add this to the ask
            taker_buffer: dec!(0.01),

            // Only requote if price moved more than this
            requote_threshold: dec!(0.01),

            // Size per order
            shares_per_trade: dec!(25.0),

            // Stop posting new quotes after this exposure
            target_shares_per_market: dec!(100.0),
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
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let dry_run = std::env::var("DRY_RUN")
            .map(|v| v == "1" || v.to_lowercase() == "false")
            .unwrap_or(false);

        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address =
            std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();

        // Allow overriding target_combined via env
        let mut legging_config = LeggingConfig::default();

        if let Ok(tc) = std::env::var("TARGET_COMBINED") {
            if let Ok(v) = tc.parse::<Decimal>() {
                legging_config.target_combined = v;
            }
        }

        if let Ok(spt) = std::env::var("SHARES_PER_TRADE") {
            if let Ok(v) = spt.parse::<Decimal>() {
                legging_config.shares_per_trade = v;
            }
        }

        if let Ok(tspm) = std::env::var("TARGET_SHARES_PER_MARKET") {
            if let Ok(v) = tspm.parse::<Decimal>() {
                legging_config.target_shares_per_market = v;
            }
        }

        Ok(Self {
            dry_run,
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets: TARGET_ASSETS.clone(),
            legging_config,
        })
    }
}