use clap::Parser;
use polymarket::bot::HighFreqArbBot;
use polymarket::config::Config;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser)]
#[command(name = "polymarket-arb")]
#[command(about = "Polymarket arbitrage bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Run in dry-run mode (no real orders)")]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    fmt().with_env_filter(filter).init();

    let mut config = Config::from_env()?;
    // Command-line flag takes precedence: set dry_run directly from args.
    config.dry_run = args.dry_run;

    tracing::info!(
        "Starting Polymarket arb bot (dry_run={}, targets={:?})",
        config.dry_run,
        config.target_assets
    );

    let mut bot = HighFreqArbBot::new(config).await?;

    bot.discover_markets().await;

    if bot.market_count() == 0 {
        tracing::warn!("No markets discovered, will retry in main loop");
    }

    for (market_id, state) in bot.markets() {
        let pair = state.pair.read();
        tracing::info!(
            "Market: {} | {} | {} | up={} down={}",
            state.info.asset,
            state.info.duration,
            market_id,
            pair.up_token_id,
            pair.down_token_id
        );
    }

    bot.run().await;

    Ok(())
}