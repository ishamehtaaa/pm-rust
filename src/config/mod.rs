use anyhow::{Context, Result};
use std::collections::HashSet;

use crate::config::assets::{ASSETS_BY_NAME, PAUSED_ASSETS, POLYMARKET_CLOB_HOST};

pub mod assets;

#[derive(Debug, Clone)]
pub struct BotConfig {
    pub clob_host: String,
    pub private_key: String,
    pub proxy_address: String,
    pub max_exposure: rust_decimal::Decimal,
    pub max_exposure_per_market: rust_decimal::Decimal,
    pub max_orders_per_second: u32,
    pub order_size: rust_decimal::Decimal,
    pub rebalance_threshold: rust_decimal::Decimal,
    pub cycle_interval_secs: u64,
    pub assets_override: Option<Vec<String>>,
    pub log_level: Option<String>,
    pub cycle_interval_override: Option<u64>,
}

impl BotConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let private_key = std::env::var("POLY_PRIVATE_KEY")
            .context("POLY_PRIVATE_KEY is required")?;
        let proxy_address = std::env::var("POLY_PROXY_ADDRESS")
            .context("POLY_PROXY_ADDRESS is required")?;

        let clob_host = std::env::var("POLYMARKET_CLOB_HOST")
            .unwrap_or_else(|_| POLYMARKET_CLOB_HOST.to_string());

        Ok(Self {
            clob_host,
            private_key,
            proxy_address,
            max_exposure: rust_decimal_macros::dec!(20.0),
            max_exposure_per_market: rust_decimal_macros::dec!(20.0),
            max_orders_per_second: 5,
            order_size: rust_decimal_macros::dec!(1.0),
            rebalance_threshold: rust_decimal_macros::dec!(20.0),
            cycle_interval_secs: 5,
            assets_override: None,
            log_level: None,
            cycle_interval_override: None,
        })
    }

    pub fn cycle_interval(&self) -> u64 {
        self.cycle_interval_override
            .unwrap_or(self.cycle_interval_secs)
    }

    pub fn target_assets(&self) -> HashSet<String> {
        if let Some(assets) = &self.assets_override {
            return assets.iter().cloned().collect();
        }

        ASSETS_BY_NAME
            .values()
            .filter(|info| !PAUSED_ASSETS.contains(info.asset.as_str()))
            .map(|info| info.asset.clone())
            .collect()
    }
}
