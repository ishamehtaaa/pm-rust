use clap::{Parser, ValueEnum};
use polymarket::bot::HighFreqArbBot;
use polymarket::config::Config;
use polymarket::legging::LeggingBot;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Strategy {
    /// Orderbook laddering bot.
    Arb,
    /// Buy both sides when combined ask < 1.00 (configurable).
    Legging,
}

#[derive(Parser)]
#[command(name = "polymarket-arb")]
#[command(about = "Polymarket arbitrage bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Run in dry-run mode (no real orders)")]
    dry_run: bool,

    #[arg(long, value_enum, default_value_t = Strategy::Legging)]
    strategy: Strategy,
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
        "Starting Polymarket bot (strategy={:?}, dry_run={}, targets={:?})",
        args.strategy,
        config.dry_run,
        config.target_assets
    );

    match args.strategy {
        Strategy::Legging => {
            let mut bot = LeggingBot::new(config).await?;
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
        }
        Strategy::Arb => {
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
        }
    }

    Ok(())
}
