use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";

pub static PAUSED_ASSETS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| HashSet::from(["ethereum"]));

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
        "solana".to_string(),
        "ethereum".to_string(),
    ])
});

#[derive(Debug, Clone)]
pub struct ArbConfig {
    pub shares_per_side: Decimal,
    pub max_limit_price: Decimal,
    pub min_profit_per_share: Decimal,
    pub arb_threshold: Decimal,
}

impl Default for ArbConfig {
    fn default() -> Self {
        Self {
            shares_per_side: dec!(20),
            max_limit_price: dec!(0.52),
            min_profit_per_share: dec!(0.03),
            arb_threshold: dec!(0.98),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dry_run: bool,
    pub polymarket_private_key: String,
    pub polymarket_proxy_address: String,
    pub target_assets: HashSet<String>,
    pub arb_config: ArbConfig,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let dry_run = std::env::var("DRY_RUN")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address = std::env::var("POLYMARKET_PROXY_ADDRESS")
            .unwrap_or_default();

        Ok(Self {
            dry_run,
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets: TARGET_ASSETS.clone(),
            arb_config: ArbConfig::default(),
        })
    }

    pub fn binance_symbols(&self) -> Vec<String> {
        self.target_assets
            .iter()
            .filter_map(|asset| ASSETS_BY_NAME.get(asset))
            .map(|info| info.binance.clone())
            .collect()
    }
}