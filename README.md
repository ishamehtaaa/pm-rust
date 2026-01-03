# pm-rust

## Secrets and configuration
- Keep `POLYMARKET_PRIVATE_KEY` (required) and `POLYMARKET_PROXY_ADDRESS` (optional) in `.env`.
- Everything else — thresholds, order sizing, opportunity buffers, target shares, etc. — belongs in `src/config.rs::LeggingConfig`.

## Running
- Dry run: `cargo run -- --dry-run`
- Live run: `cargo run`

## Strategy notes
- The bot consumes orderbook and trade websocket feeds, logs the best prices, and posts asynchronous legs whenever the combined cost is under `max_total_cost`.
- Inventory tracking keeps the up/down exposure balanced while `target_shares_per_market` limits how much we carry per market at a time.
- Learner stats are logged so you can watch success rate and score shifts as the bot experiments with different windows.
