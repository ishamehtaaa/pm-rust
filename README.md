# pm-rust

## Run

Environment:
- `POLYMARKET_PRIVATE_KEY` (required)
- `POLYMARKET_PROXY_ADDRESS` (optional)

Legging strategy (default):
- Buys both outcomes when `up_ask + down_ask <= LEGGING_MAX_TOTAL_COST` (after tick rounding/buffers).
- Config:
  - `LEGGING_MAX_TOTAL_COST` (default `0.99`)
  - `LEGGING_TAKER_BUFFER` (default `0.00`)
  - `LEGGING_COOLDOWN_SECS` (default `10`)
  - `LEGGING_MAX_PRICE_AGE_MS` (default `2500`)
  - `LEGGING_MOP_START_SECS` (default `90`)
  - `LEGGING_MOP_MAX_TOTAL_COST` (default `1.00`)
  - `LEGGING_END_TARGET_IMBALANCE_SHARES` (default `3`)
  - `LEGGING_PREVENT_ORDER_PILEUP` (default `true`)
  - `LEGGING_MANAGE_PILEUP_ORDERS` (default `true`)
  - `LEGGING_MAX_LIVE_ORDERS_PER_MARKET` (default `6`)
  - `LEGGING_MAX_LIVE_ORDERS_PER_TOKEN` (default `4`)
  - `LEGGING_EQUALIZE_MAX_TOTAL_COST` (default `0.98`)

Commands:
- Dry run: `cargo run -- --strategy legging --dry-run`
- Live: `cargo run -- --strategy legging`
- Legacy arb mode: `cargo run -- --strategy arb --dry-run`
