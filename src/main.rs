use clap::Parser;
use polymarket::bot::SimpleBot;
use polymarket::config::{ASSETS_BY_NAME, ASSETS_BY_PREFIX, Config, MarketDuration};
use rust_decimal::Decimal;
use std::collections::HashSet;
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, time::ChronoLocal},
};

#[derive(Parser)]
#[command(
    name = "polymarket",
    about = "Polymarket continuous trading bot for Up/Down markets"
)]
struct Args {
    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Dry run mode - log orders without placing them
    #[arg(long)]
    dry_run: bool,

    /// Target shares to accumulate per side (Up and Down)
    #[arg(
        long = "target",
        short = 't',
        default_value = "5000",
        value_parser = clap::value_parser!(Decimal)
    )]
    target_per_side: Decimal,

    /// Order size for each placement
    #[arg(
        long = "size",
        short = 's',
        default_value = "24",
        value_parser = clap::value_parser!(Decimal)
    )]
    order_size: Decimal,

    /// Maximum combined cost (up_avg + down_avg) to accept
    /// Lower = require more edge. 0.99 = 1% edge, 0.995 = 0.5% edge
    #[arg(
        long = "max-cost",
        default_value = "0.995",
        value_parser = clap::value_parser!(Decimal)
    )]
    max_combined_cost: Decimal,

    /// Maximum imbalance (up - down shares) before prioritizing lighter side
    #[arg(
        long = "max-imbalance",
        default_value = "200",
        value_parser = clap::value_parser!(Decimal)
    )]
    max_imbalance: Decimal,

    /// Assets to trade (comma-separated: btc,eth,sol,xrp)
    #[arg(long, value_delimiter = ',', default_value = "btc")]
    assets: Vec<String>,

    /// Market duration (15m or 1h)
    #[arg(long, default_value = "15m")]
    duration: String,
}

fn init_tracing(log_level: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let mut filter = EnvFilter::new(log_level);
        for directive in [
            "hyper_util=warn",
            "h2=warn",
            "hyper=warn",
            "reqwest=info",
            "tungstenite=warn",
            "tokio_tungstenite=warn",
        ] {
            filter = filter.add_directive(directive.parse().unwrap());
        }
        filter
    });

    fmt::Subscriber::builder()
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S".into()))
        .with_env_filter(filter)
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();
    init_tracing(&args.log_level);

    // Validate args
    if args.target_per_side <= Decimal::ZERO {
        anyhow::bail!("--target must be > 0, got {}", args.target_per_side);
    }
    if args.order_size <= Decimal::ZERO {
        anyhow::bail!("--size must be > 0, got {}", args.order_size);
    }
    if args.max_combined_cost <= Decimal::ZERO || args.max_combined_cost > Decimal::ONE {
        anyhow::bail!("--max-cost must be between 0 and 1, got {}", args.max_combined_cost);
    }

    // Build config
    let mut config = Config::from_env()?;
    config.dry_run = args.dry_run;
    config.trading.target_per_side = args.target_per_side;
    config.trading.order_size = args.order_size;
    config.trading.max_combined_cost = args.max_combined_cost;
    config.trading.max_imbalance = args.max_imbalance;

    // Parse duration
    config.target_duration = match args.duration.as_str() {
        "15m" => MarketDuration::FifteenMin,
        "1h" | "1hr" => MarketDuration::OneHour,
        other => anyhow::bail!("Unsupported duration: {} (use 15m or 1h)", other),
    };

    // Parse assets
    let mut target_assets = HashSet::new();
    for raw in args.assets {
        let key = raw.trim().to_lowercase();
        if key.is_empty() {
            continue;
        }
        if let Some(info) = ASSETS_BY_NAME.get(&key) {
            target_assets.insert(info.asset.clone());
            continue;
        }
        if let Some(info) = ASSETS_BY_PREFIX.get(&key) {
            target_assets.insert(info.asset.clone());
            continue;
        }
        anyhow::bail!("Unknown asset: {} (valid: btc, eth, sol, xrp)", raw);
    }
    if target_assets.is_empty() {
        anyhow::bail!("No valid assets provided");
    }
    config.target_assets = target_assets;

    // Log startup
    let edge_pct = (Decimal::ONE - config.trading.max_combined_cost) * Decimal::from(100);
    tracing::info!(
        dry_run = config.dry_run,
        assets = ?config.target_assets,
        duration = config.target_duration.as_str(),
        target_per_side = %config.trading.target_per_side,
        order_size = %config.trading.order_size,
        max_combined_cost = %config.trading.max_combined_cost,
        min_edge_pct = %format!("{:.2}%", edge_pct),
        max_imbalance = %config.trading.max_imbalance,
        "Starting Polymarket bot"
    );

    let mut bot = SimpleBot::new(config).await?;
    bot.run().await;
    Ok(())
}
