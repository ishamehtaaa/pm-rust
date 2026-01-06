use clap::Parser;
use polymarket::bot::LeggingBot;
use polymarket::config::{ASSETS_BY_NAME, ASSETS_BY_PREFIX, Config};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::str::FromStr;
use tracing_subscriber::{EnvFilter, fmt};
use tracing_subscriber::filter::Directive;

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
        value_parser = parse_decimal_arg,
        help = "Override target shares per market"
    )]
    shares_per_market: Option<Decimal>,

    #[arg(
        long,
        value_delimiter = ',',
        value_parser = parse_asset_arg,
        help = "Assets to include (name or prefix), comma-separated"
    )]
    target_assets: Vec<String>,
}

fn parse_decimal_arg(value: &str) -> Result<Decimal, String> {
    Decimal::from_str(value).map_err(|e| format!("invalid decimal '{}': {e}", value))
}

fn parse_asset_arg(value: &str) -> Result<String, String> {
    let key = value.trim().to_ascii_lowercase();
    if key.is_empty() {
        return Err("asset cannot be empty".to_string());
    }
    if let Some(info) = ASSETS_BY_NAME.get(&key) {
        return Ok(info.asset.clone());
    }
    if let Some(info) = ASSETS_BY_PREFIX.get(&key) {
        return Ok(info.asset.clone());
    }
    Err(format!("unsupported asset '{}'", value))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Initialize logging with tracing-subscriber
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    let filter = filter
        .add_directive("hyper_util::client::legacy::pool=off".parse::<Directive>()?)
        .add_directive("tungstenite=off".parse::<Directive>()?);
    fmt().with_env_filter(filter).init();

    // Load configuration from environment
    let mut config = Config::from_env()?;

    // Command-line flag takes precedence for dry-run mode
    if args.dry_run {
        config.dry_run = true;
    }

    if let Some(value) = args.shares_per_market {
        config.legging_config.target_shares_per_market = value;
    }

    if !args.target_assets.is_empty() {
        config.target_assets = args.target_assets.into_iter().collect::<HashSet<_>>();
    }

    tracing::info!(
        "Starting Powerful Polymarket Bot (dry_run={}, targets={:?}, shares_per_market={})",
        config.dry_run,
        config.target_assets,
        config.legging_config.target_shares_per_market
    );
    tracing::debug!(settings = ?config.bot_settings, "Bot settings");

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
