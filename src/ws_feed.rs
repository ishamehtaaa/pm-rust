use anyhow::Result;
use futures_util::stream::StreamExt;
use parking_lot::RwLock;
use polymarket_client_sdk::clob::ws;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::warn;

pub fn spawn_price_listener(
    client: ws::Client,
    asset_ids: Vec<String>,
    price_cache: Arc<RwLock<HashMap<String, (Decimal, Decimal, i64)>>>,
) -> Result<JoinHandle<()>> {
    if asset_ids.is_empty() {
        return Ok(tokio::spawn(async {}));
    }

    let handle = tokio::spawn(async move {
        let mut stream = match client.subscribe_orderbook(asset_ids) {
            Ok(stream) => Box::pin(stream),
            Err(err) => {
                warn!(%err, "Failed to subscribe to orderbook stream");
                return;
            }
        };

        while let Some(next) = stream.next().await {
            match next {
                Ok(book) => {
                    let mut cache = price_cache.write();
                    let bid = book.bids.first().map(|level| level.price);
                    let ask = book.asks.first().map(|level| level.price);

                    if bid.is_none() && ask.is_none() {
                        continue;
                    }

                    let bid_price = bid.unwrap_or(Decimal::ZERO);
                    let ask_price = ask.unwrap_or(Decimal::ZERO);
                    cache.insert(book.asset_id, (bid_price, ask_price, book.timestamp));
                }
                Err(err) => {
                    warn!(%err, "WebSocket price stream error");
                }
            }
        }
    });

    Ok(handle)
}
