use anyhow::{anyhow, bail, Result};
use clap::{Parser, ValueEnum};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;

use crate::config::assets::{SPORTS_SERIES_BY_NAME, SPORTS_SERIES_DEFAULT};
use crate::config::BotConfig;
use crate::market_cache::MarketCache;
use crate::strategy::{LadderStrategy, SportsStrategy, Strategy, StrategyParams};

#[derive(Parser, Debug)]
#[command(name = "polymarket-ladder-bot")]
struct Cli {
    /// Comma-separated asset names (e.g. bitcoin,solana)
    #[arg(long)]
    assets: Option<String>,
    /// Strategy to use
    #[arg(long, value_enum, default_value = "ladder")]
    strategy: StrategyChoice,
    /// Include high-priced markets (bid > 0.97)
    #[arg(long)]
    all: bool,
    /// Comma-separated sports series (e.g. nba,nhl)
    #[arg(long)]
    series: Option<String>,
    /// Max markets to process per cycle
    #[arg(long)]
    max_markets: Option<usize>,
    /// Max open orders per market
    #[arg(long)]
    max_open_orders: Option<usize>,
    /// Run a one-shot sanity buy (20 up/20 down) for a specific market slug
    #[arg(long)]
    sanity: bool,
    /// Market slug for sanity orders
    #[arg(long)]
    sanity_market: Option<String>,
    /// Enable pair-buy on ask when total <= threshold
    #[arg(long)]
    pair_hit: bool,
    /// Enable resting pair buys inside the spread
    #[arg(long)]
    pair_rest: bool,
    /// Shares per side for pair buys
    #[arg(long, default_value_t = 20)]
    pair_size: u32,
    /// Max total cost for pair buys (e.g. 0.99)
    #[arg(long, default_value_t = 0.95)]
    pair_threshold: f64,
    /// Tick offset for resting pair buys
    #[arg(long, default_value_t = 4)]
    pair_rest_ticks: u32,
    /// Pair mode should run without laddering
    #[arg(long)]
    pair_only: bool,
    /// Log level (e.g. info,debug,warn)
    #[arg(long)]
    log_level: Option<String>,
    /// Cycle interval (seconds)
    #[arg(long)]
    cycle_interval: Option<u64>,
    /// Websocket orderbook staleness threshold (milliseconds)
    #[arg(long)]
    book_stale_ms: Option<u64>,
    /// Max shares per side
    #[arg(long)]
    max_shares_per_side: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum StrategyChoice {
    Ladder,
    Sports,
}

pub struct AppSetup {
    pub config: BotConfig,
    pub market_cache: MarketCache,
    pub strategy: Box<dyn Strategy>,
    pub log_filter: String,
    pub include_all_markets: bool,
    pub max_markets: Option<usize>,
    pub max_open_orders: Option<usize>,
    pub sanity: Option<SanityConfig>,
    pub pair: Option<PairConfig>,
}

const SANITY_SHARES_DEFAULT: u32 = 20;

#[derive(Debug, Clone)]
pub struct SanityConfig {
    pub market_slug: String,
    pub shares_per_side: u32,
}

#[derive(Debug, Clone)]
pub struct PairConfig {
    pub hit: bool,
    pub rest: bool,
    pub shares_per_side: Decimal,
    pub threshold: Decimal,
    pub rest_ticks: u32,
    pub pair_only: bool,
}

pub fn load() -> Result<AppSetup> {
    let cli = Cli::parse();
    build_setup(cli)
}

fn build_setup(cli: Cli) -> Result<AppSetup> {
    let mut config = BotConfig::from_env()?;
    if let Some(assets) = cli.assets {
        config.assets_override = Some(parse_assets(&assets));
    }
    if let Some(level) = cli.log_level {
        config.log_level = Some(level);
    }
    if let Some(interval) = cli.cycle_interval {
        config.cycle_interval_override = Some(interval);
    }
    if let Some(stale_ms) = cli.book_stale_ms {
        config.book_stale_ms = stale_ms;
    }
    if let Some(shares) = cli.max_shares_per_side {
        config.max_shares_per_side = shares;
    }

    let log_level = config
        .log_level
        .clone()
        .unwrap_or_else(|| std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()));
    let log_filter = format!(
        "{},hyper_util=warn,reqwest=warn,tokio_tungstenite=warn",
        log_level
    );

    let market_cache = match cli.strategy {
        StrategyChoice::Ladder => MarketCache::new_crypto(config.target_assets()),
        StrategyChoice::Sports => {
            let series_ids = cli
                .series
                .as_deref()
                .map(parse_series)
                .transpose()?
                .unwrap_or_else(|| {
                    SPORTS_SERIES_DEFAULT
                        .iter()
                        .map(|id| (*id).to_string())
                        .collect()
                });
            if series_ids.is_empty() {
                bail!("SPORTS_SERIES_DEFAULT is empty");
            }
            MarketCache::new_sports(series_ids)
        }
    };

    let base_params = StrategyParams {
        base_size: config.order_size,
        rebalance_threshold: config.rebalance_threshold,
        rebalance_chunk: dec!(5.0),
        max_shares_per_side: config.max_shares_per_side,
        equilibrium_buffer: dec!(2.0),
    };

    let strategy: Box<dyn Strategy> = match cli.strategy {
        StrategyChoice::Ladder => Box::new(LadderStrategy::new(base_params.clone())),
        StrategyChoice::Sports => {
            let mut params = base_params;
            params.rebalance_chunk = dec!(2.0);
            Box::new(SportsStrategy::new(params))
        }
    };

    let sanity = if cli.sanity {
        let market_slug = cli
            .sanity_market
            .clone()
            .ok_or_else(|| anyhow!("--sanity requires --sanity-market"))?;
        Some(SanityConfig {
            market_slug,
            shares_per_side: SANITY_SHARES_DEFAULT,
        })
    } else {
        None
    };

    let pair = if cli.pair_hit || cli.pair_rest {
        Some(PairConfig {
            hit: cli.pair_hit,
            rest: cli.pair_rest,
            shares_per_side: Decimal::from(cli.pair_size),
            threshold: Decimal::from_f64(cli.pair_threshold)
                .unwrap_or_else(|| dec!(0.95)),
            rest_ticks: cli.pair_rest_ticks,
            pair_only: cli.pair_only,
        })
    } else {
        None
    };

    Ok(AppSetup {
        config,
        market_cache,
        strategy,
        log_filter,
        include_all_markets: cli.all,
        max_markets: cli.max_markets,
        max_open_orders: cli.max_open_orders,
        sanity,
        pair,
    })
}

fn parse_assets(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|asset| asset.trim().to_ascii_lowercase())
        .filter(|asset| !asset.is_empty())
        .collect()
}

fn parse_series(raw: &str) -> Result<Vec<String>> {
    let mut series_ids = Vec::new();
    for entry in raw.split(',') {
        let name = entry.trim().to_ascii_lowercase();
        if name.is_empty() {
            continue;
        }
        let ids = SPORTS_SERIES_BY_NAME
            .get(name.as_str())
            .ok_or_else(|| anyhow!("Unknown series name: {name}"))?;
        series_ids.extend(ids.iter().map(|id| (*id).to_string()));
    }
    Ok(series_ids)
}
