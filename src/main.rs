use clap::Parser;
use polymarket::bot::HighFreqArbBot;
use polymarket::clob_client::ClobClient;
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

    // Initialize logging
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&args.log_level));
    fmt().with_env_filter(filter).init();

    // Load config
    let mut config = Config::from_env()?;
    if args.dry_run {
        config.dry_run = true;
    }

    tracing::info!(
        "Starting Polymarket arb bot (dry_run={}, targets={:?})",
        config.dry_run,
        config.target_assets
    );

    // Create CLOB client (authenticates with Polymarket)
    let clob_client = ClobClient::new(&config.polymarket_private_key, false).await?;

    // Create bot
    let mut bot = HighFreqArbBot::new(config, clob_client);

    // Initial market discovery
    bot.discover_markets().await;

    if bot.market_count() == 0 {
        tracing::warn!("No markets discovered, will retry in main loop");
    }

    // Print discovered markets
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

    // Run the main trading loop
    bot.run().await;

    Ok(())
}
