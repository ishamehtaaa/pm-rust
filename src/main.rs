use clap::Parser;
use polymarket::config::Config;
use polymarket::legging::LeggingBot;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(name = "polymarket-arb")]
#[command(about = "Polymarket high-frequency legging bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Run in dry-run mode (no real orders)")]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Initialize logging with tracing-subscriber
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    fmt().with_env_filter(filter).init();

    // Load configuration from environment
    let mut config = Config::from_env()?;

    // Command-line flag takes precedence for dry-run mode
    if args.dry_run {
        config.dry_run = true;
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
