use clap::Parser;
use polymarket::bot::SimpleBot;
use polymarket::config::{ASSETS_BY_NAME, ASSETS_BY_PREFIX, Config, MarketDuration};
use rust_decimal::Decimal;
use std::collections::HashSet;
use std::fmt::Result;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, time::ChronoLocal},
};

#[derive(Parser)]
#[command(name = "polymarket-arb", about = "Polymarket arbitrage bot")]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(long)]
    dry_run: bool,

    #[arg(
        long = "shares",
        short = 's',
        default_value = "15",
        value_parser = clap::value_parser!(Decimal),
        aliases = ["shares-target-per-side"]
    )]
    shares_target_per_side: Decimal,

    #[arg(long, value_delimiter = ',', default_value = "bitcoin")]
    assets: Vec<String>,

    #[arg(long, default_value = "15m")]
    duration: String,

    #[arg(long, default_value_t = false)]
    allow_extreme_skew: bool,
}

fn init_tracing(log_level: &str) {
    let filter = build_log_filter(log_level);

    fmt::Subscriber::builder()
        .with_timer(ChronoLocal::new("%Y-%m-%d %H:%M:%S".into()))
        .with_env_filter(filter)
        .init();
}

fn build_log_filter(log_level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| {
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
    })
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let args = Args::parse();
    init_tracing(&args.log_level);

    let mut config = Config::from_env()?;
    config.dry_run = args.dry_run;
    if args.shares_target_per_side <= Decimal::ZERO {
        anyhow::bail!(
            "shares_target_per_side must be > 0, got {}",
            args.shares_target_per_side
        );
    }
    config.shares_target_per_side = args.shares_target_per_side;
    let duration = match args.duration.as_str() {
        "15m" => MarketDuration::FifteenMin,
        "1h" | "1hr" => MarketDuration::OneHour,
        other => anyhow::bail!("Unsupported duration: {}", other),
    };
    config.target_duration = duration;
    config.allow_extreme_skew = args.allow_extreme_skew;
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
        anyhow::bail!("Unknown asset: {}", raw);
    }
    if target_assets.is_empty() {
        anyhow::bail!("No valid assets provided");
    }
    config.target_assets = target_assets;

    tracing::info!(
        "Starting Polymarket bot (dry_run={}, targets={:?}, duration={})",
        config.dry_run,
        config.target_assets,
        config.target_duration.as_str()
    );

    let mut bot = SimpleBot::new(config).await?;
    bot.run().await;
    Ok(())
}
