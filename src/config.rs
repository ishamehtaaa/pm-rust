use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

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
        ("xrp", &["xrp"], "xrp/usd", "xrpusdt"),
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

#[derive(Debug, Clone)]
pub struct LeggingConfig {
    pub target_combined: Decimal,
    pub taker_buffer: Decimal,
    pub requote_threshold: Decimal,
    pub shares_per_trade: Decimal,
    pub max_shares_per_market: Decimal,
    pub max_levels: usize,
}

impl Default for LeggingConfig {
    fn default() -> Self {
        Self {
            target_combined: dec!(0.97),
            taker_buffer: dec!(0.01),
            requote_threshold: dec!(0.01),
            shares_per_trade: dec!(8.0),
            max_shares_per_market: dec!(100.0),
            max_levels: 5,
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
        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address =
            std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();

        // Optional env override: POLYMARKET_ASSETS=btc,eth
        let target_assets = match std::env::var("POLYMARKET_ASSETS") {
            Ok(v) => parse_assets(&v)?,
            Err(_) => HashSet::from(["bitcoin".to_string()]), // sane default
        };

        let mut legging_config = LeggingConfig::default();
        if let Ok(value) = std::env::var("POLYMARKET_MAX_SHARES_PER_MARKET") {
            let parsed = Decimal::from_str(&value)
                .map_err(|e| anyhow::anyhow!("invalid POLYMARKET_MAX_SHARES_PER_MARKET: {}", e))?;
            legging_config.max_shares_per_market = parsed;
        }

        Ok(Self {
            dry_run: false,
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets,
            legging_config,
        })
    }
}

/// Normalize + validate asset list from CLI or env
pub fn parse_assets(input: &str) -> anyhow::Result<HashSet<String>> {
    let mut assets = HashSet::new();

    for raw in input.split(',') {
        let key = raw.trim().to_lowercase();

        let info = ASSETS_BY_PREFIX
            .get(&key)
            .or_else(|| ASSETS_BY_NAME.get(&key))
            .ok_or_else(|| anyhow::anyhow!("Unsupported asset: {}", raw))?;

        if PAUSED_ASSETS.contains(info.asset.as_str()) {
            anyhow::bail!("Asset is paused: {}", info.asset);
        }

        assets.insert(info.asset.clone());
    }

    Ok(assets)
}
