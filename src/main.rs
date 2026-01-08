use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::sync::mpsc::unbounded_channel;
use tracing::{debug, error, info, warn};

use polymarket_client_sdk::clob::types::Side;
use polymarket_ladder_bot::config::BotConfig;
use polymarket_ladder_bot::execution::ExecutionEngine;
use polymarket_ladder_bot::inventory::{fetch_balances, reconcile_positions, InventorySnapshot};
use polymarket_ladder_bot::market_cache::MarketCache;
use polymarket_ladder_bot::risk::{OrderBudget, RiskManager};
use polymarket_ladder_bot::strategy::{LadderStrategy, MarketStage, OrderBookSnapshot, OrderKind};
use polymarket_ladder_bot::utils::{clamp_decimal, round_down_2dp, round_down_for_tick};

#[derive(Parser, Debug)]
#[command(name = "polymarket-ladder-bot")]
struct Cli {
    /// Comma-separated asset names (e.g. bitcoin,solana)
    #[arg(long)]
    assets: Option<String>,
    /// Log level (e.g. info,debug,warn)
    #[arg(long)]
    log_level: Option<String>,
    /// Max total exposure (USDC)
    #[arg(long)]
    max_exposure: Option<Decimal>,
    /// Max exposure per market (USDC)
    #[arg(long)]
    max_exposure_per_market: Option<Decimal>,
    /// Cycle interval (seconds)
    #[arg(long)]
    cycle_interval: Option<u64>,
    /// Max shares per side
    #[arg(long)]
    max_shares_per_side: Option<Decimal>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let mut config = BotConfig::from_env()?;
    if let Some(assets) = cli.assets {
        config.assets_override = Some(parse_assets(&assets));
    }
    if let Some(level) = cli.log_level {
        config.log_level = Some(level);
    }
    if let Some(exposure) = cli.max_exposure {
        config.max_exposure = exposure;
    }
    if let Some(exposure) = cli.max_exposure_per_market {
        config.max_exposure_per_market = exposure;
    }
    if let Some(interval) = cli.cycle_interval {
        config.cycle_interval_override = Some(interval);
    }
    if let Some(shares) = cli.max_shares_per_side {
        config.max_shares_per_side = shares;
    }

    let log_level = config
        .log_level
        .clone()
        .unwrap_or_else(|| std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()));
    let filter = format!(
        "{},hyper_util=warn,reqwest=warn,tokio_tungstenite=warn",
        log_level
    );

    tracing_subscriber::fmt().with_env_filter(filter).init();
    let (inventory_tx, mut inventory_rx) = unbounded_channel();
    let market_cache = MarketCache::new(config.target_assets());
    let executor = ExecutionEngine::new(&config, Some(inventory_tx))
        .await
        .context("Failed to initialize Polymarket client")?;
    let risk = RiskManager::new(config.max_exposure, config.max_exposure_per_market);
    let strategy = LadderStrategy::new(
        config.order_size,
        config.rebalance_threshold,
        dec!(5.0),
        config.max_shares_per_side,
    );

    info!(
        max_exposure = %config.max_exposure,
        max_exposure_per_market = %config.max_exposure_per_market,
        max_ops = config.max_orders_per_second,
        cycle_secs = config.cycle_interval_secs,
        order_size = %config.order_size,
        rebalance_threshold = %config.rebalance_threshold,
        "Starting ladder bot"
    );

    let mut cached_markets = Vec::new();
    let mut balance_cache: std::collections::HashMap<String, CachedBalance> =
        std::collections::HashMap::new();
    let equity_baseline: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, EquityBaseline>>,
    > = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    let mut last_market_fetch = tokio::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(600))
        .unwrap_or_else(tokio::time::Instant::now);
    let mut last_reconcile = tokio::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(120))
        .unwrap_or_else(tokio::time::Instant::now);

    loop {
        let now = Utc::now();
        if last_market_fetch.elapsed() >= std::time::Duration::from_secs(600)
            || cached_markets.is_empty()
        {
            match market_cache.get_markets(now).await {
                Ok(markets) => {
                    cached_markets = markets;
                    last_market_fetch = tokio::time::Instant::now();
                    let token_ids: Vec<String> = cached_markets
                        .iter()
                        .flat_map(|market| {
                            vec![market.ids.up_token.clone(), market.ids.down_token.clone()]
                        })
                        .collect();
                    if let Err(err) = executor.update_ws_tokens(token_ids).await {
                        warn!(error = %err, "Failed to update websocket subscriptions");
                    }
                    let market_ids: Vec<String> = cached_markets
                        .iter()
                        .map(|market| market.ids.condition_id.clone())
                        .collect();
                    if let Err(err) = executor.update_user_ws_markets(market_ids).await {
                        warn!(error = %err, "Failed to update user websocket subscriptions");
                    }
                    match reconcile_positions(executor.client(), &cached_markets).await {
                        Ok(snapshots) => {
                            let now = tokio::time::Instant::now();
                            balance_cache = snapshots
                                .into_iter()
                                .map(|(market_id, snapshot)| {
                                    (
                                        market_id,
                                        CachedBalance {
                                            snapshot,
                                            fetched_at: now,
                                        },
                                    )
                                })
                                .collect();
                            info!("Reconciled positions at startup");
                        }
                        Err(err) => {
                            warn!(error = %err, "Failed to reconcile positions");
                        }
                    }
                }
                Err(err) => {
                    error!(error = %err, "Failed to fetch markets");
                    tokio::time::sleep(std::time::Duration::from_secs(config.cycle_interval_secs))
                        .await;
                    continue;
                }
            };
        }

        debug!(count = cached_markets.len(), "Processing market batch");

        let mut log_summary = false;
        if last_reconcile.elapsed() >= std::time::Duration::from_secs(120) {
            match reconcile_positions(executor.client(), &cached_markets).await {
                Ok(snapshots) => {
                    let now = tokio::time::Instant::now();
                    balance_cache = snapshots
                        .into_iter()
                        .map(|(market_id, snapshot)| {
                            (
                                market_id,
                                CachedBalance {
                                    snapshot,
                                    fetched_at: now,
                                },
                            )
                        })
                        .collect();
                    last_reconcile = tokio::time::Instant::now();
                    info!("Reconciled positions");
                    log_summary = true;
                }
                Err(err) => {
                    warn!(error = %err, "Failed to reconcile positions");
                }
            }
        }

        for market in &cached_markets {
            let cached = balance_cache.get(&market.ids.condition_id).cloned();
            if let Err(err) = process_market(
                &executor,
                &strategy,
                &risk,
                market,
                now,
                cached,
                equity_baseline.clone(),
                log_summary,
            )
            .await
            {
                warn!(market = %market.slug, error = %err, "Market cycle failed");
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(config.cycle_interval_secs)).await;
    }
}

fn parse_assets(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|asset| asset.trim().to_ascii_lowercase())
        .filter(|asset| !asset.is_empty())
        .collect()
}

#[derive(Clone)]
struct CachedBalance {
    snapshot: InventorySnapshot,
    fetched_at: tokio::time::Instant,
}

struct EquityBaseline {
    equity: Decimal,
    updated_at: tokio::time::Instant,
}

async fn process_market(
    executor: &ExecutionEngine,
    strategy: &LadderStrategy,
    risk: &RiskManager,
    market: &polymarket_ladder_bot::models::MarketInfo,
    now: chrono::DateTime<Utc>,
    cached_balance: Option<CachedBalance>,
    equity_baseline: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, EquityBaseline>>,
    >,
    log_summary: bool,
) -> Result<()> {
    let up_book = executor.order_book(&market.ids.up_token).await?;
    let down_book = executor.order_book(&market.ids.down_token).await?;

    let (up_bid, up_ask) = resolve_best_prices(executor, &market.ids.up_token, &up_book).await?;
    let (down_bid, down_ask) =
        resolve_best_prices(executor, &market.ids.down_token, &down_book).await?;

    debug!(
        market = %market.slug,
        up_bid = %up_bid,
        up_ask = %up_ask,
        down_bid = %down_bid,
        down_ask = %down_ask,
        "Resolved best prices"
    );

    let tick_size = Decimal::from(up_book.tick_size);

    let snapshot = OrderBookSnapshot {
        up_bid,
        up_ask,
        down_bid,
        down_ask,
        up_min_size: up_book.min_order_size,
        down_min_size: down_book.min_order_size,
        tick_size,
    };

    debug!(
        market = %market.slug,
        up_min_size = %snapshot.up_min_size,
        down_min_size = %snapshot.down_min_size,
        tick_size = %snapshot.tick_size,
        "Market constraints"
    );

    let balances = match cached_balance {
        Some(cache) if cache.fetched_at.elapsed() < std::time::Duration::from_secs(30) => {
            cache.snapshot
        }
        _ => fetch_balances(
            executor.client(),
            &market.ids.up_token,
            &market.ids.down_token,
        )
        .await
        .context("Failed to fetch balances")?,
    };

    debug!(
        market = %market.slug,
        up_balance = %balances.up_balance,
        down_balance = %balances.down_balance,
        usdc_balance = %balances.usdc_balance,
        "Fetched balances"
    );

    let stage = strategy.stage_for_market(now, market);
    let target_total = strategy.target_total_shares(now, market);
    let target_side = target_total / dec!(2.0);
    debug!(
        market = %market.slug,
        target_total = %target_total,
        target_side = %target_side,
        "Target inventory"
    );

    let stats = executor.user_stats().await;
    debug!(
        orders_seen = stats.orders_seen,
        trades_seen = stats.trades_seen,
        last_trade_market = ?stats.last_trade_market,
        last_trade_size = ?stats.last_trade_size,
        matched_size_total = %stats.matched_size_total,
        "User fill stats"
    );

    let exposure_cap = risk.budget_limit_per_market(balances.usdc_balance);
    let mark_value = balances.up_balance * up_bid + balances.down_balance * down_bid;
    let exposure_remaining = exposure_cap - mark_value;
    let equity_estimate = balances.usdc_balance + mark_value;
    let pnl_estimate = {
        let mut baseline = equity_baseline.lock().await;
        let entry = baseline
            .entry(market.ids.condition_id.clone())
            .or_insert_with(|| EquityBaseline {
                equity: equity_estimate,
                updated_at: tokio::time::Instant::now(),
            });
        if entry.updated_at.elapsed() >= std::time::Duration::from_secs(120) {
            entry.equity = equity_estimate;
            entry.updated_at = tokio::time::Instant::now();
        }
        equity_estimate - entry.equity
    };

    if log_summary {
        info!(
            market = %market.slug,
            up_bid = %up_bid,
            up_ask = %up_ask,
            down_bid = %down_bid,
            down_ask = %down_ask,
            up_balance = %balances.up_balance,
            down_balance = %balances.down_balance,
            net_delta = %(balances.up_balance - balances.down_balance),
            target_side = %target_side,
            mark_value_usdc = %mark_value,
            net_value_usdc = %(balances.up_balance * up_bid - balances.down_balance * down_bid),
            usdc_balance = %balances.usdc_balance,
            equity_estimate = %equity_estimate,
            pnl_estimate = %pnl_estimate,
            exposure_cap = %exposure_cap,
            exposure_remaining = %exposure_remaining,
            "Inventory summary"
        );
    }
    let mut orders = strategy.build_orders(
        market,
        &snapshot,
        stage,
        balances.up_balance,
        balances.down_balance,
    );
    let mut projected_up = balances.up_balance;
    let mut projected_down = balances.down_balance;
    let mut iteration = 0;
    while iteration < 3 {
        let delta = projected_up - projected_down;
        if delta.abs() <= strategy.equilibrium_buffer() {
            break;
        }
        let extra = strategy.build_orders(
            market,
            &snapshot,
            MarketStage::Early,
            projected_up,
            projected_down,
        );
        if extra.is_empty() {
            break;
        }
        for intent in &extra {
            if intent.token_id == market.ids.up_token {
                projected_up += intent.size;
            } else {
                projected_down += intent.size;
            }
        }
        orders.extend(extra);
        iteration += 1;
    }
    orders =
        strategy.apply_share_limits(orders, balances.up_balance, balances.down_balance, market);

    if orders.is_empty() {
        if log_summary {
            info!(market = %market.slug, "No orders generated for market");
        } else {
            debug!(market = %market.slug, "No orders generated for market");
        }
        return Ok(());
    }

    debug!(
        market = %market.slug,
        count = orders.len(),
        stage = ?stage,
        "Generated ladder orders"
    );

    executor
        .cancel_orders_for_token(&market.ids.up_token)
        .await?;
    executor
        .cancel_orders_for_token(&market.ids.down_token)
        .await?;

    let budget_limit = risk.budget_limit_per_market(balances.usdc_balance);
    let mut budget = OrderBudget::new(budget_limit);

    for order in orders {
        let notional = if let Some(amount) = order.market_usdc {
            amount
        } else {
            round_down_2dp(order.price * order.size)
        };
        if order.kind == OrderKind::Ladder && !budget.try_reserve(notional) {
            warn!(
                market = %market.slug,
                price = %order.price,
                size = %order.size,
                remaining = %budget.remaining(),
                "Skipping order due to exposure limit"
            );
            continue;
        }

        let order_id = if let Some(amount) = order.market_usdc {
            executor
                .place_market_order_usdc(&order.token_id, amount)
                .await?
        } else {
            executor
                .place_limit_order(&order.token_id, order.price, order.size)
                .await?
        };

        info!(
            market = %market.slug,
            price = %order.price,
            size = %order.size,
            reason = %order.reason,
            "Placed order"
        );
        debug!(order_id = %order_id);
    }

    Ok(())
}

fn best_bid(
    book: &polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse,
) -> Option<Decimal> {
    book.bids.iter().map(|level| level.price).max()
}

fn best_ask(
    book: &polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse,
) -> Option<Decimal> {
    book.asks.iter().map(|level| level.price).min()
}

async fn resolve_best_prices(
    executor: &ExecutionEngine,
    token_id: &str,
    book: &polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse,
) -> Result<(Decimal, Decimal)> {
    let mut bid = None;
    let mut ask = None;

    if let Some((cached_bid, cached_ask)) = executor.cached_best_prices(token_id).await {
        bid = Some(cached_bid);
        ask = Some(cached_ask);
    }

    if bid.is_none() || ask.is_none() {
        bid = bid.or_else(|| best_bid(book));
        ask = ask.or_else(|| best_ask(book));
    }

    if bid.is_none() {
        if let Ok(price) = executor.price(token_id, Side::Buy).await {
            bid = Some(price);
            tracing::debug!(token_id = %token_id, bid = %price, "Filled bid from price endpoint");
        }
    }

    if ask.is_none() {
        if let Ok(price) = executor.price(token_id, Side::Sell).await {
            ask = Some(price);
            tracing::debug!(token_id = %token_id, ask = %price, "Filled ask from price endpoint");
        }
    }

    if bid.is_some() && ask.is_some() {
        return Ok((bid.unwrap(), ask.unwrap()));
    }

    let tick = Decimal::from(book.tick_size);
    if let Ok(mid) = executor.midpoint(token_id).await {
        let mid = round_down_for_tick(mid, tick);
        if bid.is_none() {
            let candidate = clamp_decimal(mid - tick, tick, dec!(0.99));
            bid = Some(candidate);
            tracing::debug!(token_id = %token_id, bid = %candidate, "Synthesized bid from midpoint");
        }
        if ask.is_none() {
            let candidate = clamp_decimal(mid + tick, tick, dec!(0.99));
            ask = Some(candidate);
            tracing::debug!(token_id = %token_id, ask = %candidate, "Synthesized ask from midpoint");
        }
    }

    let mut bid = bid.map(|value| clamp_decimal(value, tick, dec!(0.99)));
    let mut ask = ask.map(|value| clamp_decimal(value, tick, dec!(0.99)));
    if let (Some(bid_value), Some(ask_value)) = (bid, ask) {
        if bid_value >= ask_value {
            let adjusted_bid = clamp_decimal(ask_value - tick, tick, dec!(0.99));
            let adjusted_ask = clamp_decimal(adjusted_bid + tick, tick, dec!(0.99));
            bid = Some(adjusted_bid);
            ask = Some(adjusted_ask);
        }
    }

    match (bid, ask) {
        (Some(bid), Some(ask)) => Ok((bid, ask)),
        _ => Err(anyhow::anyhow!("Missing bid/ask after fallback")),
    }
}
