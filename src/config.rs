use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, HashSet};

pub const POLYMARKET_CLOB_HOST: &str = "https://clob.polymarket.com";

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
    Lazy::new(|| HashSet::from(["solana".to_string()]));

pub fn resolve_target_assets<I>(assets: I) -> anyhow::Result<HashSet<String>>
where
    I: IntoIterator<Item = String>,
{
    let mut resolved = HashSet::new();
    let mut unknown = Vec::new();

    for asset in assets {
        let trimmed = asset.trim().to_lowercase();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(info) = ASSETS_BY_PREFIX
            .get(&trimmed)
            .or_else(|| ASSETS_BY_NAME.get(&trimmed))
        {
            resolved.insert(info.asset.clone());
        } else {
            unknown.push(asset);
        }
    }

    if !unknown.is_empty() {
        return Err(anyhow::anyhow!(
            "Unknown asset(s): {}",
            unknown.join(", ")
        ));
    }

    Ok(resolved)
}

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
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let dry_run = std::env::var("DRY_RUN")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        let polymarket_private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY env var is required"))?;

        let polymarket_proxy_address =
            std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();

        let target_assets = match std::env::var("TARGET_ASSETS") {
            Ok(v) => resolve_target_assets(v.split(',').map(|s| s.to_string()))?,
            Err(_) => TARGET_ASSETS.clone(),
        };

        let shares_target_per_side = parse_decimal_env("SHARES_TARGET_PER_SIDE", dec!(25))?;
        let order_size = parse_decimal_env("ORDER_SIZE", dec!(5))?;
        let target_total_cost = parse_decimal_env("TARGET_TOTAL_COST", dec!(0.97))?;
        let maker_price_offset = parse_decimal_env("MAKER_PRICE_OFFSET", dec!(0.01))?;
        let max_price_age_ms = parse_i64_env("MAX_PRICE_AGE_MS", 2_500)?;
        let cooldown_secs = parse_u64_env("COOLDOWN_SECS", 2)?;

        if shares_target_per_side <= Decimal::ZERO {
            return Err(anyhow::anyhow!(
                "SHARES_TARGET_PER_SIDE must be > 0, got {}",
                shares_target_per_side
            ));
        }
        if order_size <= Decimal::ZERO {
            return Err(anyhow::anyhow!(
                "ORDER_SIZE must be > 0, got {}",
                order_size
            ));
        }
        if target_total_cost <= Decimal::ZERO || target_total_cost >= dec!(1.00) {
            return Err(anyhow::anyhow!(
                "TARGET_TOTAL_COST must be in (0, 1.00), got {}",
                target_total_cost
            ));
        }
        if maker_price_offset < Decimal::ZERO {
            return Err(anyhow::anyhow!(
                "MAKER_PRICE_OFFSET must be >= 0, got {}",
                maker_price_offset
            ));
        }
        if max_price_age_ms <= 0 {
            return Err(anyhow::anyhow!(
                "MAX_PRICE_AGE_MS must be > 0, got {}",
                max_price_age_ms
            ));
        }

        Ok(Self {
            dry_run,
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets,
            shares_target_per_side,
            order_size,
            target_total_cost,
            maker_price_offset,
            max_price_age_ms,
            cooldown_secs,
        })
    }
}

fn parse_decimal_env(key: &str, default: Decimal) -> anyhow::Result<Decimal> {
    match std::env::var(key) {
        Ok(v) => Decimal::from_str_exact(&v).map_err(|e| anyhow::anyhow!("Invalid {}: {}", key, e)),
        Err(_) => Ok(default),
    }
}

fn parse_i64_env(key: &str, default: i64) -> anyhow::Result<i64> {
    match std::env::var(key) {
        Ok(v) => v
            .parse::<i64>()
            .map_err(|e| anyhow::anyhow!("Invalid {}: {}", key, e)),
        Err(_) => Ok(default),
    }
}

fn parse_u64_env(key: &str, default: u64) -> anyhow::Result<u64> {
    match std::env::var(key) {
        Ok(v) => v
            .parse::<u64>()
            .map_err(|e| anyhow::anyhow!("Invalid {}: {}", key, e)),
        Err(_) => Ok(default),
    }
}
