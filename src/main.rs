use clap::Parser;
use polymarket::config::{Config, parse_assets};
use polymarket::legging::LeggingBot;
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(name = "polymarket-arb")]
#[command(about = "Polymarket high-frequency legging bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Run in dry-run mode (no real orders)")]
    dry_run: bool,

    #[arg(
        long,
        value_delimiter = ',',
        num_args = 1..,
        help = "Comma-separated list of assets to monitor (prefixes or names)"
    )]
    target_assets: Vec<String>,

    #[arg(long, help = "Max ladder levels per side", value_parser = clap::value_parser!(usize))]
    max_levels: Option<usize>,

    #[arg(
        long,
        help = "Share size per ladder order",
        value_parser = parse_decimal
    )]
    shares_per_trade: Option<Decimal>,

    #[arg(
        long,
        help = "Max shares (per side) allowed before the trailing side triggers rebalancing",
        value_parser = parse_decimal
    )]
    max_shares_per_market: Option<Decimal>,
}

fn parse_decimal(value: &str) -> Result<Decimal, String> {
    Decimal::from_str(value).map_err(|err| format!("invalid decimal value: {}", err))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Initialize logging with tracing-subscriber
    let mut filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    filter = filter
        .add_directive("hyper_util::client::legacy::pool=warn".parse().unwrap())
        .add_directive("tungstenite::handshake::client=warn".parse().unwrap());
    fmt().with_env_filter(filter).init();

    // Load configuration from environment
    let mut config = Config::from_env()?;

    // Command-line flag takes precedence for dry-run mode
    if args.dry_run {
        config.dry_run = true;
    }

    let filtered_assets: Vec<&str> = args
        .target_assets
        .iter()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if !filtered_assets.is_empty() {
        let normalized = filtered_assets.join(",");
        config.target_assets = parse_assets(&normalized)?;
    }

    if let Some(max_levels) = args.max_levels {
        config.legging_config.max_levels = max_levels;
    }

    if let Some(shares_per_trade) = args.shares_per_trade {
        config.legging_config.shares_per_trade = shares_per_trade;
    }

    if let Some(max_shares_per_market) = args.max_shares_per_market {
        config.legging_config.max_shares_per_market = max_shares_per_market;
    }

    tracing::info!(
        "Starting Powerful Polymarket Bot (dry_run={}, targets={:?})",
        config.dry_run,
        config.target_assets
    );

    // Initialize the bot
    let mut bot = LeggingBot::new(config).await?;

    // Discover initial markets
    bot.discover_markets().await;

    if bot.market_count() == 0 {
        tracing::warn!(
            "No active markets discovered during startup. Will keep searching in the background."
        );
    }

    // Log the initial state of discovered markets
    for (market_id, state) in bot.markets() {
        tracing::info!(
            "Monitored Market: {} | {} | Asset: {} | End: {}",
            market_id,
            state.info.slug,
            state.info.asset,
            state.info.end_time
        );
    }

    // Start the high-frequency trading loop
    // This calls the scan_markets method and manages the WS/REST event loop
    bot.run().await;

    Ok(())
}
