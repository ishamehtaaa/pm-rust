use crate::models::{FillEvent, MarketLookup, OrderEvent};
use futures_util::stream::StreamExt;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::ws::{self, BookUpdate, WsMessage};
use rust_decimal::Decimal;
use serde_json::to_string;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, trace, warn};

#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub token_id: String,
    pub bid: Decimal,
    pub ask: Decimal,
}

pub fn spawn_orderbook_task(
    client: ws::Client,
    tokens: Vec<String>,
    price_tx: mpsc::Sender<PriceUpdate>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(500);
        loop {
            match client.subscribe_orderbook(tokens.clone()) {
                Ok(sub) => {
                    info!("Subscribed to orderbook");
                    backoff = Duration::from_millis(500);
                    let mut stream = Box::pin(sub);

                    while let Some(msg) = stream.next().await {
                        match msg {
                            Ok(BookUpdate {
                                asset_id,
                                bids,
                                asks,
                                ..
                            }) => {
                                let bid = bids
                                    .iter()
                                    .max_by_key(|l| l.price)
                                    .and_then(|l| l.price.to_string().parse().ok());
                                let ask = asks
                                    .iter()
                                    .min_by_key(|l| l.price)
                                    .and_then(|l| l.price.to_string().parse().ok());

                                if let (Some(bid), Some(ask)) = (bid, ask) {
                                    if let Err(err) = price_tx.try_send(PriceUpdate {
                                        token_id: asset_id.clone(),
                                        bid,
                                        ask,
                                    }) {
                                        trace!(%asset_id, error = ?err, "Dropping price update (channel full)");
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Orderbook error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => warn!("Orderbook subscribe failed: {}", e),
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    })
}

pub fn spawn_user_events_task(
    client: ws::Client<Authenticated<Normal>>,
    conditions: Vec<String>,
    lookup: MarketLookup,
    order_tx: mpsc::Sender<OrderEvent>,
    fill_tx: mpsc::Sender<FillEvent>,
    price_tx: mpsc::Sender<PriceUpdate>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let order_channel = order_tx;
        let fill_channel = fill_tx;
        let mut backoff = Duration::from_millis(500);
        loop {
            match client.subscribe_user_events(conditions.clone()) {
                Ok(sub) => {
                    info!("Subscribed to user events");
                    backoff = Duration::from_millis(500);
                    let mut stream = Box::pin(sub);

                    while let Some(msg) = stream.next().await {
                        match msg {
                            Ok(WsMessage::PriceChange(ref change)) => {
                                trace!(
                                    market = %change.market,
                                    count = change.price_changes.len(),
                                    "User WS price change"
                                );
                                let gamma_id = match lookup.resolve_condition(&change.market) {
                                    Some(id) => id,
                                    None => continue,
                                };
                                if lookup.get_ids(gamma_id).is_none() {
                                    continue;
                                }

                                for entry in &change.price_changes {
                                    if let (Some(bid), Some(ask)) = (entry.best_bid, entry.best_ask)
                                    {
                                        if let Err(err) = price_tx.try_send(PriceUpdate {
                                            token_id: entry.asset_id.clone(),
                                            bid,
                                            ask,
                                        }) {
                                            warn!(
                                                asset = %entry.asset_id,
                                                error = ?err,
                                                "Dropping user price update (channel full)"
                                            );
                                        }
                                    }
                                }
                            }
                            Ok(WsMessage::Order(ref o)) => {
                                let order_payload =
                                    to_string(o).unwrap_or_else(|e| format!("<json error: {e}>"));
                                tracing::debug!(payload = %order_payload, "User WS order event");
                                let gamma_id = match lookup.resolve_condition(&o.market) {
                                    Some(id) => id.to_string(),
                                    None => continue,
                                };

                                let side = match lookup
                                    .get_ids(&gamma_id)
                                    .and_then(|ids| ids.side_for_token(&o.asset_id))
                                {
                                    Some(s) => s,
                                    None => continue,
                                };

                                let price = match o.price.to_string().parse() {
                                    Ok(p) => p,
                                    Err(_) => continue,
                                };

                                let order_gamma = gamma_id.clone();
                                if order_channel
                                    .send(OrderEvent {
                                        order_id: o.id.clone(),
                                        gamma_id: order_gamma,
                                        token_id: o.asset_id.clone(),
                                        side,
                                        price,
                                        size_matched: Decimal::from(
                                            o.size_matched.unwrap_or_default(),
                                        ),
                                        msg_type: o.msg_type.clone().unwrap_or_default(),
                                    })
                                    .await
                                    .is_err()
                                {
                                    warn!(
                                        order_id = %o.id,
                                        gamma_id = %gamma_id,
                                        "Order channel closed"
                                    );
                                    break;
                                }
                            }
                            Ok(WsMessage::Trade(ref t)) => {
                                let trade_payload =
                                    to_string(t).unwrap_or_else(|e| format!("<json error: {e}>"));
                                trace!(payload = %trade_payload, "User WS trade event");

                                if t.status != "MATCHED" {
                                    continue;
                                }

                                let gamma_id = match lookup.resolve_condition(&t.market) {
                                    Some(id) => id.to_string(),
                                    None => continue,
                                };

                                use polymarket_client_sdk::clob::types::TraderSide;
                                let is_taker = t.trader_side == Some(TraderSide::Taker);
                                if is_taker {
                                    continue;
                                }

                                // Process all maker_orders - these represent filled amounts
                                for mo in &t.maker_orders {
                                    let side = match lookup
                                        .get_ids(&gamma_id)
                                        .and_then(|ids| ids.side_for_token(&mo.asset_id))
                                    {
                                        Some(s) => s,
                                        None => continue,
                                    };

                                    let price: Decimal = match mo.price.to_string().parse() {
                                        Ok(p) => p,
                                        Err(_) => continue,
                                    };

                                    let matched: Decimal =
                                        match mo.matched_amount.to_string().parse() {
                                            Ok(m) => m,
                                            Err(_) => continue,
                                        };

                                    if matched <= Decimal::ZERO {
                                        continue;
                                    }

                                    if fill_channel
                                        .send(FillEvent {
                                            trade_id: t.id.clone(),
                                            gamma_id: gamma_id.clone(),
                                            token_id: mo.asset_id.clone(),
                                            order_id: mo.order_id.clone(),
                                            side,
                                            price,
                                            size: matched,
                                            is_taker,
                                        })
                                        .await
                                        .is_err()
                                    {
                                        warn!(
                                            order_id = %mo.order_id,
                                            gamma_id = %gamma_id,
                                            "Fill channel closed"
                                        );
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("User events error: {}", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => warn!("User events subscribe failed: {}", e),
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    })
}
