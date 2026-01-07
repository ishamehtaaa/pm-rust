use anyhow::Result;
use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk::clob::types::AssetType;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::types::Decimal;

#[derive(Debug, Clone)]
pub struct InventorySnapshot {
    pub up_balance: Decimal,
    pub down_balance: Decimal,
    pub usdc_balance: Decimal,
}

pub async fn fetch_balances(
    client: &Client<Authenticated<Normal>>,
    up_token: &str,
    down_token: &str,
) -> Result<InventorySnapshot> {
    let up_balance = client
        .balance_allowance(
            BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Conditional)
                .token_id(up_token)
                .build(),
        )
        .await?
        .balance;

    let down_balance = client
        .balance_allowance(
            BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Conditional)
                .token_id(down_token)
                .build(),
        )
        .await?
        .balance;

    let usdc_balance = client
        .balance_allowance(
            BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Collateral)
                .build(),
        )
        .await?
        .balance;

    Ok(InventorySnapshot {
        up_balance: up_balance / Decimal::from(1_000_000u64),
        down_balance: down_balance / Decimal::from(1_000_000u64),
        usdc_balance: usdc_balance / Decimal::from(1_000_000u64),
    })
}
