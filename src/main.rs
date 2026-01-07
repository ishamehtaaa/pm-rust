use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{error, info, warn, debug};

use polymarket_client_sdk::clob::types::Side;
use polymarket_ladder_bot::config::BotConfig;
use polymarket_ladder_bot::execution::ExecutionEngine;
use polymarket_ladder_bot::inventory::fetch_balances;
use polymarket_ladder_bot::market_cache::MarketCache;
use polymarket_ladder_bot::risk::{OrderBudget, RiskManager};
use polymarket_ladder_bot::strategy::{LadderStrategy, OrderBookSnapshot};
use polymarket_ladder_bot::utils::{clamp_decimal, round_down_2dp, round_down_for_tick};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        )
        .init();

    let config = BotConfig::from_env()?;
    let market_cache = MarketCache::new(config.target_assets());
    let executor = ExecutionEngine::new(&config)
        .await
        .context("Failed to initialize Polymarket client")?;
    let risk = RiskManager::new(config.max_exposure);
    let strategy = LadderStrategy::new(config.order_size, config.rebalance_threshold);

    info!(
        max_exposure = %config.max_exposure,
        max_ops = config.max_orders_per_second,
        cycle_secs = config.cycle_interval_secs,
        order_size = %config.order_size,
        rebalance_threshold = %config.rebalance_threshold,
        "Starting ladder bot"
    );

    let mut cached_markets = Vec::new();
    let mut last_market_fetch = tokio::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(600))
        .unwrap_or_else(tokio::time::Instant::now);

    loop {
        let now = Utc::now();
        if last_market_fetch.elapsed() >= std::time::Duration::from_secs(600) || cached_markets.is_empty() {
            match market_cache.get_markets(now).await {
                Ok(markets) => {
                    cached_markets = markets;
                    last_market_fetch = tokio::time::Instant::now();
                    let token_ids: Vec<String> = cached_markets
                        .iter()
                        .flat_map(|market| {
                            vec![
                                market.ids.up_token.clone(),
                                market.ids.down_token.clone(),
                            ]
                        })
                        .collect();
                    if let Err(err) = executor.update_ws_tokens(token_ids).await {
                        warn!(error = %err, "Failed to update websocket subscriptions");
                    }
                }
                Err(err) => {
                    error!(error = %err, "Failed to fetch markets");
                    tokio::time::sleep(std::time::Duration::from_secs(config.cycle_interval_secs)).await;
                    continue;
                }
            };
        }

        info!(count = cached_markets.len(), "Processing market batch");

        for market in &cached_markets {
            if let Err(err) = process_market(
                &executor,
                &strategy,
                &risk,
                market,
                now,
            )
            .await
            {
                warn!(market = %market.slug, error = %err, "Market cycle failed");
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(config.cycle_interval_secs)).await;
    }
}

async fn process_market(
    executor: &ExecutionEngine,
    strategy: &LadderStrategy,
    risk: &RiskManager,
    market: &polymarket_ladder_bot::models::MarketInfo,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    let up_book = executor.order_book(&market.ids.up_token).await?;
    let down_book = executor.order_book(&market.ids.down_token).await?;

    let (up_bid, up_ask) =
        resolve_best_prices(executor, &market.ids.up_token, &up_book).await?;
    let (down_bid, down_ask) =
        resolve_best_prices(executor, &market.ids.down_token, &down_book).await?;

    info!(
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

    info!(
        market = %market.slug,
        up_min_size = %snapshot.up_min_size,
        down_min_size = %snapshot.down_min_size,
        tick_size = %snapshot.tick_size,
        "Market constraints"
    );

    let balances = fetch_balances(executor.client(), &market.ids.up_token, &market.ids.down_token)
        .await
        .context("Failed to fetch balances")?;

    info!(
        market = %market.slug,
        up_balance = %balances.up_balance,
        down_balance = %balances.down_balance,
        usdc_balance = %balances.usdc_balance,
        "Fetched balances"
    );

    let stage = strategy.stage_for_market(now, market);
    let target_total = strategy.target_total_shares(now, market);
    let target_side = target_total / dec!(2.0);
    info!(
        market = %market.slug,
        target_total = %target_total,
        target_side = %target_side,
        "Target inventory"
    );
    let orders = strategy.build_orders(
        market,
        &snapshot,
        stage,
        balances.up_balance,
        balances.down_balance,
    );

    if orders.is_empty() {
        info!(market = %market.slug, "No orders generated for market");
        return Ok(());
    }

    info!(
        market = %market.slug,
        count = orders.len(),
        stage = ?stage,
        "Generated ladder orders"
    );

    executor.cancel_orders_for_token(&market.ids.up_token).await?;
    executor.cancel_orders_for_token(&market.ids.down_token).await?;

    let budget_limit = risk.budget_limit(balances.usdc_balance);
    let mut budget = OrderBudget::new(budget_limit);

    for order in orders {
        let notional = if let Some(amount) = order.market_usdc {
            amount
        } else {
            round_down_2dp(order.price * order.size)
        };
        if !budget.try_reserve(notional) {
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

fn best_bid(book: &polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse) -> Option<Decimal> {
    book.bids.iter().map(|level| level.price).max()
}

fn best_ask(book: &polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse) -> Option<Decimal> {
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
