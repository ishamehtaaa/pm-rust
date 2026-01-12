use alloy::primitives::Address;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::AssetType;
use polymarket_client_sdk::clob::types::request::{BalanceAllowanceRequest, TradesRequest};
use polymarket_client_sdk::clob::types::response::{Page, TradeResponse};
use rust_decimal::Decimal;
use tracing::{error, info};

use crate::constants::to_shares;

type AuthenticatedClient = Client<Authenticated<Normal>>;

pub async fn fetch_token_balances(
    client: &AuthenticatedClient,
    up_token_id: &str,
    down_token_id: &str,
) -> anyhow::Result<(Decimal, Decimal)> {
    let up_req = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Conditional)
        .token_id(up_token_id)
        .build();

    let down_req = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Conditional)
        .token_id(down_token_id)
        .build();

    let up_resp = client.balance_allowance(up_req).await?;
    let down_resp = client.balance_allowance(down_req).await?;

    let up_raw: Decimal = up_resp.balance.to_string().parse().unwrap_or_default();
    let down_raw: Decimal = down_resp.balance.to_string().parse().unwrap_or_default();

    /* The API returns some crazy format so we need to convert this to a valid share amount. */
    let up_bal = to_shares(up_raw);
    let down_bal = to_shares(down_raw);

    Ok((up_bal, down_bal))
}

pub async fn get_trades(
    client: &AuthenticatedClient,
    token_id: &str,
) -> Option<Page<TradeResponse>> {
    /* Filter the trade request for trades where we are the maker. */
    let address = client.address();
    let trade_request = TradesRequest::builder()
        .asset_id(token_id)
        .maker_address(address)
        .build();

    match client.trades(&trade_request, None).await {
        Ok(page) => {
            for trade in &page.data {
                info!(
                    side = ?trade.side,
                    size = ?trade.size,
                    status = ?trade.status
                );
            }
            Some(page)
        }
        Err(e) => {
            error!(error = %e, "failed to fetch trades");
            None
        }
    }
}
