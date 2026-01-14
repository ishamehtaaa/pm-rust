# pm-rust

## Run

Environment:
- `POLYMARKET_PRIVATE_KEY` (required)
- `POLYMARKET_PROXY_ADDRESS` (optional)

Commands:
- Dry run: `cargo run -- --dry-run`
- Live: `cargo run`
- Override target shares per side: `cargo run -- --dry-run --shares-target-per-side 30`
- Target assets: `cargo run -- --dry-run --assets btc,eth`
