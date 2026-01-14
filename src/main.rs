use clap::Parser;
use polymarket::bot::SimpleBot;
use polymarket::config::{Config, MarketDuration, resolve_target_assets};
use std::str::FromStr;
use tracing_subscriber::{
    EnvFilter,
    fmt::time::ChronoLocal,
};

#[derive(Parser)]
#[command(name = "polymarket-arb")]
#[command(about = "Polymarket arbitrage bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Run in dry-run mode (no real orders)")]
    dry_run: bool,

    #[arg(
        long,
        value_delimiter = ',',
        help = "Comma-separated asset names or prefixes to target"
    )]
    assets: Option<Vec<String>>,

    #[arg(
        long,
        help = "Market duration to target: 15m or 1hr"
    )]
    duration: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(&args.log_level)
            .add_directive("hyper_util=warn".parse().unwrap())
            .add_directive("h2=warn".parse().unwrap())
            .add_directive("hyper=warn".parse().unwrap())
            .add_directive("reqwest=info".parse().unwrap())
            .add_directive("tungstenite=warn".parse().unwrap())
            .add_directive("tokio_tungstenite=warn".parse().unwrap())
    });

    let mut config = Config::from_env()?;
    config.dry_run = args.dry_run;

    if let Some(assets) = args.assets {
        let resolved = resolve_target_assets(assets)?;
        if resolved.is_empty() {
            return Err(anyhow::anyhow!(
                "No valid assets provided via --assets"
            ));
        }
        config.target_assets = resolved;
    }

    if let Some(duration) = args.duration {
        config.market_duration = MarketDuration::from_str(&duration)?;
    }

    /* Initialize the logger with a custom timestamp and quieting noisy logs. */
    tracing_subscriber::fmt()
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S".into()))
        .with_env_filter(filter)
        .init();

    tracing::info!(
        "Starting Polymarket bot (dry_run={}, duration={}, targets={:?})",
        config.dry_run,
        config.market_duration,
        config.target_assets
    );

    let mut bot = SimpleBot::new(config).await?;
    bot.discover_markets().await;

    if bot.market_count() == 0 {
        tracing::warn!("No markets discovered, will retry in main loop");
    }

    for (market_id, state) in bot.markets() {
        tracing::info!("Market: {} | {}", state.asset, market_id);
    }

    bot.run().await;

    Ok(())
}
