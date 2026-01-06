use clap::Parser;
use polymarket::bot::HighFreqArbBot;
use polymarket::config::Config;
use rust_decimal::Decimal;
use std::collections::HashSet;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser)]
#[command(name = "polymarket-arb")]
#[command(about = "Polymarket arbitrage bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long, help = "Run in dry-run mode (no real orders)")]
    dry_run: bool,

    #[arg(long, help = "Ladder levels per side (default: 2)")]
    ladder_levels: Option<usize>,

    #[arg(long, help = "Max filled shares per side for a market")]
    max_shares_per_side: Option<Decimal>,

    #[arg(long, help = "Per-market cooldown in seconds")]
    cooldown_secs: Option<u64>,

    #[arg(long, help = "Comma-separated asset list (e.g. bitcoin,solana)")]
    assets: Option<String>,
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
    if let Some(levels) = args.ladder_levels {
        config.legging_config.max_live_orders_per_token = levels.max(1);
    }
    if let Some(max_shares_per_side) = args.max_shares_per_side {
        config.legging_config.max_shares_per_side = max_shares_per_side;
    }
    if let Some(cooldown_secs) = args.cooldown_secs {
        config.legging_config.cooldown_secs = cooldown_secs;
    }
    if let Some(assets) = args.assets {
        let mut set: HashSet<String> = HashSet::new();
        for asset in assets.split(',') {
            let trimmed = asset.trim();
            if !trimmed.is_empty() {
                set.insert(trimmed.to_lowercase());
            }
        }
        if !set.is_empty() {
            config.target_assets = set;
        }
    }

    tracing::info!(
        "Starting Polymarket bot (strategy=arb, dry_run={}, targets={:?})",
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
