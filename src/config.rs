
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
    ])
});

#[derive(Debug, Clone)]
pub struct ArbConfig {
    pub shares_per_side: Decimal,
    /// The threshold factor for considering an arbitrage (e.g., 0.98 means we require the combined price <= 0.98 * reference)
    pub arb_threshold: Decimal,
}

impl Default for ArbConfig {
    fn default() -> Self {
        Self {
            shares_per_side: dec!(10),
            arb_threshold: dec!(0.98),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeggingConfig {
    /// Max combined cost to buy both outcomes (after rounding/buffers).
    pub max_total_cost: Decimal,
    /// Extra price buffer added to the observed ask to improve fill probability.
    pub taker_buffer: Decimal,
    /// Per-market cooldown to avoid repeated buys on the same opportunity.
    pub cooldown_secs: u64,
    /// Skip prices older than this many milliseconds.
    pub max_price_age_ms: i64,
    /// When remaining time is below this, prioritize flattening inventory.
    pub mop_start_secs: i64,
    /// Max combined cost per paired share during mop (avg cost of excess side + current ask).
    pub mop_max_total_cost: Decimal,
    /// Target end-of-market imbalance in shares (absolute).
    pub end_target_imbalance_shares: Decimal,
    /// If true, prevent placing new orders while you have live orders in the market.
    pub prevent_order_pileup: bool,
    /// Enable aggressive cancellation + equalization when open orders pile up.
    pub manage_pileup_orders: bool,
    /// If open orders exceed this per market, trigger cancellation logic.
    pub max_live_orders_per_market: usize,
    /// If open orders exceed this for a single token, trigger cancellation logic.
    pub max_live_orders_per_token: usize,
    /// Max combined cost per paired share when equalizing mid-market (should be < 1.00 to be net-positive).
    pub equalize_max_total_cost: Decimal,
}

impl Default for LeggingConfig {
    fn default() -> Self {
        Self {
            max_total_cost: dec!(0.98),
            taker_buffer: dec!(0.00),
            cooldown_secs: 1,
            max_price_age_ms: 2_500,
            mop_start_secs: 90,
            mop_max_total_cost: dec!(1.00),
            end_target_imbalance_shares: dec!(3),
            prevent_order_pileup: true,
            manage_pileup_orders: true,
            max_live_orders_per_market: 6,
            max_live_orders_per_token: 4,
            equalize_max_total_cost: dec!(0.98),
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
    pub legging_config: LeggingConfig,
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

        let mut legging_config = LeggingConfig::default();
        if let Ok(v) = std::env::var("LEGGING_MAX_TOTAL_COST") {
            legging_config.max_total_cost = Decimal::from_str_exact(&v)
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_MAX_TOTAL_COST: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_TAKER_BUFFER") {
            legging_config.taker_buffer = Decimal::from_str_exact(&v)
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_TAKER_BUFFER: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_COOLDOWN_SECS") {
            legging_config.cooldown_secs = v
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_COOLDOWN_SECS: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_MAX_PRICE_AGE_MS") {
            legging_config.max_price_age_ms = v
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_MAX_PRICE_AGE_MS: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_MOP_START_SECS") {
            legging_config.mop_start_secs = v
                .parse::<i64>()
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_MOP_START_SECS: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_MOP_MAX_TOTAL_COST") {
            legging_config.mop_max_total_cost = Decimal::from_str_exact(&v)
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_MOP_MAX_TOTAL_COST: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_END_TARGET_IMBALANCE_SHARES") {
            legging_config.end_target_imbalance_shares = Decimal::from_str_exact(&v)
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_END_TARGET_IMBALANCE_SHARES: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_PREVENT_ORDER_PILEUP") {
            legging_config.prevent_order_pileup = v == "true" || v == "1";
        }
        if let Ok(v) = std::env::var("LEGGING_MANAGE_PILEUP_ORDERS") {
            legging_config.manage_pileup_orders = v == "true" || v == "1";
        }
        if let Ok(v) = std::env::var("LEGGING_MAX_LIVE_ORDERS_PER_MARKET") {
            legging_config.max_live_orders_per_market = v
                .parse::<usize>()
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_MAX_LIVE_ORDERS_PER_MARKET: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_MAX_LIVE_ORDERS_PER_TOKEN") {
            legging_config.max_live_orders_per_token = v
                .parse::<usize>()
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_MAX_LIVE_ORDERS_PER_TOKEN: {}", e))?;
        }
        if let Ok(v) = std::env::var("LEGGING_EQUALIZE_MAX_TOTAL_COST") {
            legging_config.equalize_max_total_cost = Decimal::from_str_exact(&v)
                .map_err(|e| anyhow::anyhow!("Invalid LEGGING_EQUALIZE_MAX_TOTAL_COST: {}", e))?;
        }

        if legging_config.max_total_cost <= Decimal::ZERO || legging_config.max_total_cost > dec!(1.00) {
            return Err(anyhow::anyhow!(
                "LEGGING_MAX_TOTAL_COST must be in (0, 1.00], got {}",
                legging_config.max_total_cost
            ));
        }
        if legging_config.taker_buffer < Decimal::ZERO {
            return Err(anyhow::anyhow!(
                "LEGGING_TAKER_BUFFER must be >= 0, got {}",
                legging_config.taker_buffer
            ));
        }
        if legging_config.max_price_age_ms <= 0 {
            return Err(anyhow::anyhow!(
                "LEGGING_MAX_PRICE_AGE_MS must be > 0, got {}",
                legging_config.max_price_age_ms
            ));
        }
        if legging_config.mop_start_secs <= 0 {
            return Err(anyhow::anyhow!(
                "LEGGING_MOP_START_SECS must be > 0, got {}",
                legging_config.mop_start_secs
            ));
        }
        if legging_config.mop_max_total_cost <= Decimal::ZERO || legging_config.mop_max_total_cost > dec!(1.05) {
            return Err(anyhow::anyhow!(
                "LEGGING_MOP_MAX_TOTAL_COST must be in (0, 1.05], got {}",
                legging_config.mop_max_total_cost
            ));
        }
        if legging_config.end_target_imbalance_shares < Decimal::ZERO {
            return Err(anyhow::anyhow!(
                "LEGGING_END_TARGET_IMBALANCE_SHARES must be >= 0, got {}",
                legging_config.end_target_imbalance_shares
            ));
        }
        if legging_config.max_live_orders_per_market == 0 {
            return Err(anyhow::anyhow!(
                "LEGGING_MAX_LIVE_ORDERS_PER_MARKET must be > 0"
            ));
        }
        if legging_config.max_live_orders_per_token == 0 {
            return Err(anyhow::anyhow!(
                "LEGGING_MAX_LIVE_ORDERS_PER_TOKEN must be > 0"
            ));
        }
        if legging_config.equalize_max_total_cost <= Decimal::ZERO || legging_config.equalize_max_total_cost >= dec!(1.00) {
            return Err(anyhow::anyhow!(
                "LEGGING_EQUALIZE_MAX_TOTAL_COST must be in (0, 1.00), got {}",
                legging_config.equalize_max_total_cost
            ));
        }

        Ok(Self {
            dry_run,
            polymarket_private_key,
            polymarket_proxy_address,
            target_assets: TARGET_ASSETS.clone(),
            arb_config: ArbConfig::default(),
            legging_config,
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
