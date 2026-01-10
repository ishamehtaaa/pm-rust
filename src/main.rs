use anyhow::{Context, Result};
use chrono::Utc;
use rust_decimal::Decimal;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use polymarket_ladder_bot::app;
use polymarket_ladder_bot::balances::{ActualBalances, PendingBalances};
use polymarket_ladder_bot::execution::{
    BookSnapshot, ExecutionEngine, LimitOrderRequest, OpenOrderView,
};
use polymarket_ladder_bot::inventory::{fetch_balances, reconcile_positions, InventorySnapshot};
use polymarket_ladder_bot::models::MarketInfo;
use polymarket_ladder_bot::strategy::{OrderBookSnapshot, OrderIntent, Strategy};
use rust_decimal_macros::dec;
use std::hash::{Hash, Hasher};

const MARKET_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const BALANCE_CACHE_TTL: Duration = Duration::from_secs(30);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(120);
const FULL_SCAN_INTERVAL: Duration = Duration::from_secs(10);
const MAX_BID_CUTOFF: Decimal = dec!(0.95);
const MIN_BID_CUTOFF: Decimal = dec!(0.05);
const MAX_PLACEMENT_FAILURES: usize = 2;
const SANITY_CHUNK_SIZE: Decimal = dec!(5.0);
const SANITY_MAX_AVG: Decimal = dec!(0.95);
const SIZE_EPSILON: Decimal = dec!(0.0001);
const PAIR_IMBALANCE_LIMIT: Decimal = dec!(20.0);

#[tokio::main]
async fn main() -> Result<()> {
    let setup = app::load()?;
    let config = setup.config;
    let timer = tracing_subscriber::fmt::time::UtcTime::new(
        time::format_description::parse("[year]-[month]-[day]T[hour]:[minute]:[second]Z")
            .unwrap(),
    );
    tracing_subscriber::fmt()
        .with_env_filter(setup.log_filter)
        .with_target(false)
        .with_timer(timer)
        .init();
    let (inventory_tx, _inventory_rx) = mpsc::unbounded_channel();
    let market_cache = setup.market_cache;
    let include_all_markets = setup.include_all_markets;
    let max_markets = setup.max_markets;
    let max_open_orders = setup.max_open_orders;
    let sanity = setup.sanity.clone();
    let pair = setup.pair.clone();
    let (book_tx, mut book_rx) = mpsc::unbounded_channel();
    let executor = ExecutionEngine::new(&config, Some(inventory_tx), Some(book_tx))
        .await
        .context("Failed to initialize Polymarket client")?;
    let strategy = setup.strategy;
    let book_stale_ms = config.book_stale_ms;

    let cycle_delay = Duration::from_secs(config.cycle_interval());
    info!(
        max_ops = config.max_orders_per_second,
        cycle_secs = %cycle_delay.as_secs(),
        order_size = %config.order_size,
        rebalance_threshold = %config.rebalance_threshold,
        "Starting ladder bot"
    );

    let mut cached_markets = Vec::new();
    let mut balance_cache: std::collections::HashMap<String, CachedBalance> =
        std::collections::HashMap::new();
    let mut last_books: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    let mut missing_books: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut skip_logs: std::collections::HashMap<String, (String, tokio::time::Instant)> =
        std::collections::HashMap::new();
    let mut token_to_market: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut market_index: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let equity_baseline: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, EquityBaseline>>,
    > = std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new()));
    let mut last_market_fetch = tokio::time::Instant::now()
        .checked_sub(MARKET_REFRESH_INTERVAL)
        .unwrap_or_else(tokio::time::Instant::now);
    let mut last_reconcile = tokio::time::Instant::now()
        .checked_sub(RECONCILE_INTERVAL)
        .unwrap_or_else(tokio::time::Instant::now);
    let mut last_full_scan = tokio::time::Instant::now()
        .checked_sub(FULL_SCAN_INTERVAL)
        .unwrap_or_else(tokio::time::Instant::now);

    loop {
        let _cycle_started = tokio::time::Instant::now();
        let now = Utc::now();
        if last_market_fetch.elapsed() >= MARKET_REFRESH_INTERVAL
            || cached_markets.is_empty()
        {
            match market_cache.get_markets(now).await {
                Ok(markets) => {
                    cached_markets = markets;
                    if let Some(limit) = max_markets {
                        if cached_markets.len() > limit {
                            cached_markets.truncate(limit);
                            info!(count = cached_markets.len(), "Limiting markets for cycle");
                        }
                    }
                    token_to_market.clear();
                    market_index.clear();
                    for (idx, market) in cached_markets.iter().enumerate() {
                        token_to_market
                            .insert(market.ids.up_token.clone(), market.ids.condition_id.clone());
                        token_to_market
                            .insert(market.ids.down_token.clone(), market.ids.condition_id.clone());
                        market_index.insert(market.ids.condition_id.clone(), idx);
                    }
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
                    tokio::time::sleep(cycle_delay).await;
                    continue;
                }
            };
        }
        if cached_markets.iter().any(|market| market.end_time <= now) {
            info!("Detected expired market window; refreshing market list");
            last_market_fetch = tokio::time::Instant::now()
                .checked_sub(MARKET_REFRESH_INTERVAL)
                .unwrap_or_else(tokio::time::Instant::now);
            continue;
        }

        let mut dirty_markets: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        if let Ok(Some(token_id)) = tokio::time::timeout(cycle_delay, book_rx.recv()).await {
            if let Some(market_id) = token_to_market.get(&token_id) {
                dirty_markets.insert(market_id.clone());
            }
            let debounce_deadline = tokio::time::Instant::now()
                .checked_add(Duration::from_millis(50))
                .unwrap_or_else(tokio::time::Instant::now);
            loop {
                let timeout = debounce_deadline
                    .checked_duration_since(tokio::time::Instant::now())
                    .unwrap_or_else(|| Duration::from_millis(0));
                if timeout.is_zero() {
                    break;
                }
                match tokio::time::timeout(timeout, book_rx.recv()).await {
                    Ok(Some(token_id)) => {
                        if let Some(market_id) = token_to_market.get(&token_id) {
                            dirty_markets.insert(market_id.clone());
                        }
                    }
                    _ => break,
                }
            }
        }

        debug!(
            count = cached_markets.len(),
            dirty = dirty_markets.len(),
            "Processing market batch"
        );

        let mut log_summary = false;
        if last_reconcile.elapsed() >= RECONCILE_INTERVAL {
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

        if let Some(sanity) = &sanity {
            let target = cached_markets.iter().find(|market| {
                market.slug == sanity.market_slug
                    || market.asset.eq_ignore_ascii_case(&sanity.market_slug)
            });
            if let Some(target) = target {
                if let Err(err) =
                    run_sanity_order(&executor, target, sanity.shares_per_side).await
                {
                    warn!(market = %target.slug, error = %err, "Sanity order failed");
                }
                break;
            } else {
                warn!(
                    market = %sanity.market_slug,
                    "Sanity market not found in current market list"
                );
            }
        }

        let mut targets: Vec<&MarketInfo> = Vec::new();
        let force_full_scan = last_full_scan.elapsed() >= FULL_SCAN_INTERVAL;
        if dirty_markets.is_empty() || force_full_scan {
            targets.extend(cached_markets.iter());
            last_full_scan = tokio::time::Instant::now();
        } else {
            for market_id in dirty_markets {
                if let Some(idx) = market_index.get(&market_id) {
                    if let Some(market) = cached_markets.get(*idx) {
                        targets.push(market);
                    }
                }
            }
        }

        for market in targets {
            let cached = balance_cache.get(&market.ids.condition_id).cloned();
            if let Err(err) = process_market(
                &executor,
                strategy.as_ref(),
                market,
                now,
                cached,
                equity_baseline.clone(),
                log_summary,
                include_all_markets,
                max_open_orders,
                &mut last_books,
                &mut missing_books,
                &mut skip_logs,
                book_stale_ms,
                pair.as_ref(),
            )
            .await
            {
                warn!(market = %market.slug, error = %err, "Market cycle failed");
            }
        }

        tokio::time::sleep(cycle_delay).await;
    }

    Ok(())
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
    strategy: &dyn Strategy,
    market: &polymarket_ladder_bot::models::MarketInfo,
    now: chrono::DateTime<Utc>,
    cached_balance: Option<CachedBalance>,
    equity_baseline: std::sync::Arc<
        tokio::sync::Mutex<std::collections::HashMap<String, EquityBaseline>>,
    >,
    log_summary: bool,
    include_all_markets: bool,
    max_open_orders: Option<usize>,
    last_books: &mut std::collections::HashMap<String, u64>,
    missing_books: &mut std::collections::HashSet<String>,
    skip_logs: &mut std::collections::HashMap<String, (String, tokio::time::Instant)>,
    book_stale_ms: u64,
    pair: Option<&polymarket_ladder_bot::app::PairConfig>,
) -> Result<()> {
    let _market_started = tokio::time::Instant::now();
    let Some(up_book) = executor.cached_book(&market.ids.up_token).await else {
        let key = format!("{}:up", market.ids.condition_id);
        if missing_books.insert(key) {
            warn!(
                market = %market.slug,
                "Missing websocket orderbook (up)"
            );
        }
        return Ok(());
    };
    let Some(down_book) = executor.cached_book(&market.ids.down_token).await else {
        let key = format!("{}:down", market.ids.condition_id);
        if missing_books.insert(key) {
            warn!(
                market = %market.slug,
                "Missing websocket orderbook (down)"
            );
        }
        return Ok(());
    };

    let now_ms = now.timestamp_millis();
    let stale_threshold = book_stale_ms as i64;
    if now_ms - up_book.updated_at > stale_threshold
        || now_ms - down_book.updated_at > stale_threshold
    {
        if should_log_skip(
            skip_logs,
            market,
            "stale orderbook",
            std::time::Duration::from_secs(20),
        ) {
            warn!(
                market = %market.slug,
                up_age_ms = now_ms - up_book.updated_at,
                down_age_ms = now_ms - down_book.updated_at,
                threshold_ms = book_stale_ms,
                "Skipping market due to stale orderbook"
            );
        }
        return Ok(());
    }

    let up_bid = up_book
        .bids
        .iter()
        .map(|level| level.price)
        .max()
        .unwrap_or(Decimal::ZERO);
    let up_ask = up_book
        .asks
        .iter()
        .map(|level| level.price)
        .min()
        .unwrap_or(Decimal::ZERO);
    let down_bid = down_book
        .bids
        .iter()
        .map(|level| level.price)
        .max()
        .unwrap_or(Decimal::ZERO);
    let down_ask = down_book
        .asks
        .iter()
        .map(|level| level.price)
        .min()
        .unwrap_or(Decimal::ZERO);

    if up_bid <= Decimal::ZERO
        || up_ask <= Decimal::ZERO
        || down_bid <= Decimal::ZERO
        || down_ask <= Decimal::ZERO
        || up_bid >= up_ask
        || down_bid >= down_ask
    {
        debug!(
            market = %market.slug,
            up_bid = %up_bid,
            up_ask = %up_ask,
            down_bid = %down_bid,
            down_ask = %down_ask,
            "Skipping market due to invalid top-of-book"
        );
        return Ok(());
    }

    let up_meta = executor.token_meta(&market.ids.up_token).await?;
    let down_meta = executor.token_meta(&market.ids.down_token).await?;
    let up_momentum = executor
        .momentum(&market.ids.up_token)
        .await
        .unwrap_or(Decimal::ZERO);
    let down_momentum = executor
        .momentum(&market.ids.down_token)
        .await
        .unwrap_or(Decimal::ZERO);

    if !include_all_markets && (up_bid > dec!(0.97) || down_bid > dec!(0.97)) {
        debug!(
            market = %market.slug,
            up_bid = %up_bid,
            down_bid = %down_bid,
            "Skipping high-priced market"
        );
        return Ok(());
    }
    if up_bid >= MAX_BID_CUTOFF
        || down_bid >= MAX_BID_CUTOFF
        || up_bid <= MIN_BID_CUTOFF
        || down_bid <= MIN_BID_CUTOFF
    {
        if should_log_skip(
            skip_logs,
            market,
            "bid thresholds",
            std::time::Duration::from_secs(30),
        ) {
            debug!(
                market = %market.slug,
                up_bid = %up_bid,
                down_bid = %down_bid,
                "Skipping market due to bid thresholds"
            );
        }
        return Ok(());
    }

    let tick_size = up_meta.tick_size;

    let snapshot = OrderBookSnapshot {
        up_bid,
        up_ask,
        down_bid,
        down_ask,
        up_min_size: up_meta.min_order_size,
        down_min_size: down_meta.min_order_size,
        tick_size,
        up_momentum,
        down_momentum,
    };


    let balances = resolve_balances(executor, market, cached_balance).await?;
    let actual = ActualBalances {
        up: balances.up_balance,
        down: balances.down_balance,
        usdc: balances.usdc_balance,
    };
    let pending = PendingBalances {
        up: executor.pending_size(&market.ids.up_token).await,
        down: executor.pending_size(&market.ids.down_token).await,
    };
    let effective = actual.with_pending(pending);
    let imbalance = (effective.up - effective.down).abs();

    if let Some(pair) = pair {
        if pair.hit || pair.rest {
            if let Err(err) = run_pair_orders(
                executor,
                market,
                &snapshot,
                pair,
                effective,
            )
            .await
            {
                warn!(market = %market.slug, error = %err, "Pair order failed");
            }
            if pair.pair_only {
                return Ok(());
            }
        }
    }

    if imbalance < strategy.rebalance_threshold() {
        if let Some(hash) = combine_book_hash(&up_book, &down_book) {
            let key = market.ids.condition_id.clone();
            if let Some(last_hash) = last_books.get(&key) {
                if *last_hash == hash {
                    if should_log_skip(
                        skip_logs,
                        market,
                        "unchanged book",
                        std::time::Duration::from_secs(30),
                    ) {
                        debug!(market = %market.slug, "Skipping market due to unchanged book");
                    }
                    return Ok(());
                }
            }
            last_books.insert(key, hash);
        }
    }

    let stage = strategy.stage_for_market(now, market);
    let target_total = strategy.target_total_shares(now, market);
    let target_side = target_total / dec!(2.0);
    let mark_value = actual.up * up_bid + actual.down * down_bid;
    let equity_estimate = actual.usdc + mark_value;
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
            down_bid = %down_bid,
            up_balance = %actual.up,
            down_balance = %actual.down,
            pending_up = %pending.up,
            pending_down = %pending.down,
            target_side = %target_side,
            mark_value_usdc = %mark_value,
            usdc_balance = %actual.usdc,
            equity_estimate = %equity_estimate,
            pnl_estimate = %pnl_estimate,
            "Inventory summary"
        );
    }
    debug!(
        market = %market.slug,
        stage = ?stage,
        up_bid = %up_bid,
        up_ask = %up_ask,
        down_bid = %down_bid,
        down_ask = %down_ask,
        up_min_size = %snapshot.up_min_size,
        down_min_size = %snapshot.down_min_size,
        tick_size = %snapshot.tick_size,
        up_balance = %actual.up,
        down_balance = %actual.down,
        pending_up = %pending.up,
        pending_down = %pending.down,
        usdc_balance = %actual.usdc,
        target_total = %target_total,
        target_side = %target_side,
        "Market snapshot"
    );
    let mut result = strategy.build_orders_with_result(market, &snapshot, stage, effective);
    result.orders = strategy.apply_share_limits(result.orders, effective, market);

    if result.orders.is_empty() {
        if should_log_skip(
            skip_logs,
            market,
            "no orders",
            std::time::Duration::from_secs(30),
        ) {
            debug!(
                market = %market.slug,
                outcome = ?result.outcome,
                "No orders generated for market"
            );
        }
        return Ok(());
    }

    debug!(
        market = %market.slug,
        count = result.orders.len(),
        outcome = ?result.outcome,
        "Generated orders"
    );

    let mut limit_orders = Vec::new();
    let mut market_orders = FuturesUnordered::new();

    for order in result.orders {
        if let Some(amount) = order.market_usdc {
            let executor = executor;
            market_orders.push(async move {
                let placement = executor
                    .place_market_order_usdc(&order.token_id, amount)
                    .await;
                (order, placement)
            });
        } else {
            limit_orders.push(order);
        }
    }

    let open_orders = executor
        .open_orders_for_tokens(&[market.ids.up_token.as_str(), market.ids.down_token.as_str()])
        .await;
    let open_orders_len = open_orders.len();
    let price_epsilon = snapshot.tick_size / dec!(2.0);
    let (to_cancel, mut to_place) =
        diff_limit_orders(limit_orders, open_orders, price_epsilon, SIZE_EPSILON);

    if let Some(limit) = max_open_orders {
        let remaining_after_cancel = open_orders_len.saturating_sub(to_cancel.len());
        let max_new = limit.saturating_sub(remaining_after_cancel);
        let desired_len = to_place.len();
        if desired_len > max_new {
            debug!(
                market = %market.slug,
                desired = desired_len,
                allowed = max_new,
                "Capping new orders due to open order limit"
            );
            to_place.truncate(max_new);
        }
        if max_new == 0 && desired_len > 0 {
            info!(
                market = %market.slug,
                open_orders = open_orders_len,
                max_open_orders = limit,
                "Skipping placements due to open order limit"
            );
        }
    }

    if !to_cancel.is_empty() {
        let cancelled = executor.cancel_orders(&to_cancel).await?;
        debug!(
            market = %market.slug,
            cancelled,
            "Cancelled stale orders"
        );
    }

    let mut failures = 0usize;
    let mut up_placed = 0usize;
    let mut down_placed = 0usize;
    if !to_place.is_empty() {
        let batch_requests = to_place
            .iter()
            .map(|order| LimitOrderRequest {
                token_id: order.token_id.clone(),
                price: order.price,
                size: order.size,
            })
            .collect();
        let responses = executor.place_limit_orders(batch_requests).await?;
        for (order, response) in to_place.into_iter().zip(responses.into_iter()) {
            if !response.success {
                warn!(
                    market = %market.slug,
                    price = %order.price,
                    size = %order.size,
                    reason = %order.reason,
                    kind = ?order.kind,
                    error = ?response.error_msg,
                    "Order failed"
                );
                failures += 1;
                if failures >= MAX_PLACEMENT_FAILURES {
                    warn!(
                        market = %market.slug,
                        failures,
                        "Halting placements due to failures"
                    );
                    break;
                }
                continue;
            }
            let side = if order.token_id == market.ids.up_token {
                up_placed += 1;
                "UP"
            } else {
                down_placed += 1;
                "DOWN"
            };
            info!(
                market = %market.slug,
                side,
                price = %order.price,
                size = %order.size,
                reason = %order.reason,
                "Order placed"
            );
            debug!(order_id = %response.order_id);
        }
    }

    if failures >= MAX_PLACEMENT_FAILURES {
        market_orders.clear();
    }

    while let Some((order, placement)) = market_orders.next().await {
        let order_id = match placement {
            Ok(id) => id,
            Err(err) => {
                warn!(
                    market = %market.slug,
                    price = %order.price,
                    size = %order.size,
                    reason = %order.reason,
                    kind = ?order.kind,
                    error = %err,
                    "Order failed"
                );
                failures += 1;
                if failures >= MAX_PLACEMENT_FAILURES {
                    warn!(
                        market = %market.slug,
                        failures,
                        "Halting placements due to failures"
                    );
                    break;
                }
                continue;
            }
        };

        let side = if order.token_id == market.ids.up_token {
            up_placed += 1;
            "UP"
        } else {
            down_placed += 1;
            "DOWN"
        };
        info!(
            market = %market.slug,
            side,
            price = %order.price,
            size = %order.size,
            reason = %order.reason,
            "Order placed"
        );
        debug!(order_id = %order_id);
    }

    info!(
        market = %market.slug,
        up_orders = up_placed,
        down_orders = down_placed,
        "Order summary"
    );

    Ok(())
}

fn should_log_skip(
    cache: &mut std::collections::HashMap<String, (String, tokio::time::Instant)>,
    market: &polymarket_ladder_bot::models::MarketInfo,
    reason: &str,
    interval: std::time::Duration,
) -> bool {
    let now = tokio::time::Instant::now();
    let key = market.ids.condition_id.clone();
    match cache.get(&key) {
        Some((prev, ts)) if prev == reason && ts.elapsed() < interval => false,
        _ => {
            cache.insert(key, (reason.to_string(), now));
            true
        }
    }
}

fn diff_limit_orders(
    intents: Vec<OrderIntent>,
    open_orders: Vec<OpenOrderView>,
    price_epsilon: Decimal,
    size_epsilon: Decimal,
) -> (Vec<String>, Vec<OrderIntent>) {
    let mut remaining = intents;
    let mut to_cancel = Vec::new();

    for open in open_orders {
        let maybe_idx = remaining.iter().position(|intent| {
            intent.token_id == open.token_id
                && (intent.price - open.price).abs() <= price_epsilon
        });
        let Some(idx) = maybe_idx else {
            to_cancel.push(open.id);
            continue;
        };

        let desired = remaining[idx].size;
        let delta = open.remaining - desired;
        if delta.abs() <= size_epsilon {
            remaining.remove(idx);
        } else if delta < Decimal::ZERO {
            remaining[idx].size = desired - open.remaining;
        } else {
            to_cancel.push(open.id);
        }
    }

    (to_cancel, remaining)
}

async fn resolve_balances(
    executor: &ExecutionEngine,
    market: &MarketInfo,
    cached_balance: Option<CachedBalance>,
) -> Result<InventorySnapshot> {
    if let Some(cache) = cached_balance {
        if cache.fetched_at.elapsed() < BALANCE_CACHE_TTL {
            return Ok(cache.snapshot);
        }
    }

    fetch_balances(
        executor.client(),
        &market.ids.up_token,
        &market.ids.down_token,
    )
    .await
    .context("Failed to fetch balances")
}

async fn run_sanity_order(
    executor: &ExecutionEngine,
    market: &MarketInfo,
    shares_per_side: u32,
) -> Result<()> {
    let Some(up_book) = executor.cached_book(&market.ids.up_token).await else {
        return Err(anyhow::anyhow!("Missing websocket orderbook for up token"));
    };
    let Some(down_book) = executor.cached_book(&market.ids.down_token).await else {
        return Err(anyhow::anyhow!("Missing websocket orderbook for down token"));
    };

    let up_ask = up_book
        .asks
        .first()
        .map(|level| level.price)
        .unwrap_or(Decimal::ZERO);
    let down_ask = down_book
        .asks
        .first()
        .map(|level| level.price)
        .unwrap_or(Decimal::ZERO);
    if up_ask <= Decimal::ZERO || down_ask <= Decimal::ZERO {
        return Err(anyhow::anyhow!("Missing ask price for sanity order"));
    }

    let avg = (up_ask + down_ask) / dec!(2.0);
    if avg > SANITY_MAX_AVG {
        return Err(anyhow::anyhow!(
            "Sanity order blocked: avg price {} > {}",
            avg,
            SANITY_MAX_AVG
        ));
    }

    let mut remaining = Decimal::from(shares_per_side);
    let mut batch = Vec::new();
    while remaining > Decimal::ZERO {
        let size = if remaining >= SANITY_CHUNK_SIZE {
            SANITY_CHUNK_SIZE
        } else {
            remaining
        };
        if size < SANITY_CHUNK_SIZE {
            break;
        }
        batch.push(LimitOrderRequest {
            token_id: market.ids.up_token.clone(),
            price: up_ask,
            size,
        });
        batch.push(LimitOrderRequest {
            token_id: market.ids.down_token.clone(),
            price: down_ask,
            size,
        });
        remaining -= size;
    }

    if batch.is_empty() {
        return Err(anyhow::anyhow!("Sanity order size too small"));
    }

    let responses = executor.place_limit_orders(batch).await?;
    for response in responses {
        if !response.success {
            return Err(anyhow::anyhow!(
                "Sanity order failed: {:?}",
                response.error_msg
            ));
        }
    }

    info!(market = %market.slug, "Sanity order submitted");
    Ok(())
}

async fn run_pair_orders(
    executor: &ExecutionEngine,
    market: &MarketInfo,
    snapshot: &OrderBookSnapshot,
    pair: &polymarket_ladder_bot::app::PairConfig,
    balances: polymarket_ladder_bot::balances::EffectiveBalances,
) -> Result<()> {
    let up_ask = snapshot.up_ask;
    let down_ask = snapshot.down_ask;
    let total = up_ask + down_ask;

    let min_size = snapshot.up_min_size.max(snapshot.down_min_size);
    let size = pair.shares_per_side.max(min_size);
    let imbalance = balances.up - balances.down;
    let only_weaker = imbalance.abs() > PAIR_IMBALANCE_LIMIT;
    let buy_up = !only_weaker || imbalance < Decimal::ZERO;
    let buy_down = !only_weaker || imbalance > Decimal::ZERO;

    let mut orders = Vec::new();
    if pair.hit {
        if total > pair.threshold {
            debug!(
                market = %market.slug,
                up_ask = %up_ask,
                down_ask = %down_ask,
                total = %total,
                threshold = %pair.threshold,
                "Pair hit skipped: total exceeds threshold"
            );
        } else {
            if buy_up {
                orders.push(LimitOrderRequest {
                    token_id: market.ids.up_token.clone(),
                    price: up_ask,
                    size,
                });
            }
            if buy_down {
                orders.push(LimitOrderRequest {
                    token_id: market.ids.down_token.clone(),
                    price: down_ask,
                    size,
                });
            }
        }
    }

    if pair.rest {
        let tick = snapshot.tick_size;
        let offset = Decimal::from(pair.rest_ticks);
        let rest_up = (up_ask - tick * offset).max(tick);
        let rest_down = (down_ask - tick * offset).max(tick);
        let rest_total = rest_up + rest_down;
        if rest_total <= pair.threshold {
            if buy_up {
                orders.push(LimitOrderRequest {
                    token_id: market.ids.up_token.clone(),
                    price: rest_up,
                    size,
                });
            }
            if buy_down {
                orders.push(LimitOrderRequest {
                    token_id: market.ids.down_token.clone(),
                    price: rest_down,
                    size,
                });
            }
        } else {
            debug!(
                market = %market.slug,
                rest_up = %rest_up,
                rest_down = %rest_down,
                rest_total = %rest_total,
                threshold = %pair.threshold,
                "Pair rest skipped: total exceeds threshold"
            );
        }
    }

    if orders.is_empty() {
        debug!(
            market = %market.slug,
            "Pair skipped: no eligible sides (imbalance or limits)"
        );
        return Ok(());
    }

    let responses = executor.place_limit_orders(orders).await?;
    for response in responses {
        if !response.success {
            return Err(anyhow::anyhow!(
                "Pair order failed: {:?}",
                response.error_msg
            ));
        }
    }

    info!(market = %market.slug, "Pair orders submitted");
    Ok(())
}

fn combine_book_hash(up: &BookSnapshot, down: &BookSnapshot) -> Option<u64> {
    if up.bids.is_empty() || up.asks.is_empty() || down.bids.is_empty() || down.asks.is_empty() {
        return None;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hash_book(up, &mut hasher);
    hash_book(down, &mut hasher);
    Some(hasher.finish())
}

fn hash_book(book: &BookSnapshot, hasher: &mut impl Hasher) {
    for level in &book.bids {
        level.price.to_string().hash(hasher);
        level.size.to_string().hash(hasher);
    }
    for level in &book.asks {
        level.price.to_string().hash(hasher);
        level.size.to_string().hash(hasher);
    }
}
