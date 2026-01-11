use clap::Parser;
use polymarket::bot::SimpleBot;
use polymarket::config::Config;
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

    tracing::info!(
        "Starting Polymarket bot (dry_run={}, targets={:?})",
        config.dry_run,
        config.target_assets
    );

    let mut bot = SimpleBot::new(config).await?;
    bot.discover_markets().await;

    if bot.market_count() == 0 {
        tracing::warn!("No markets discovered, will retry in main loop");
    }

    for (market_id, state) in bot.markets() {
        tracing::info!(
            "Market: {} | {} | {}",
            state.info.asset,
            state.info.duration,
            market_id,
        );
    }

    bot.run().await;
    Ok(())
}


