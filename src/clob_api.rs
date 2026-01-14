use parking_lot::RwLock;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::Client;
use polymarket_client_sdk::clob::types::AssetType;
use polymarket_client_sdk::clob::types::request::{BalanceAllowanceRequest, TradesRequest};
use polymarket_client_sdk::clob::types::response::{Page, TradeResponse};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::constants::to_shares;

type AuthenticatedClient = Client<Authenticated<Normal>>;

/// Cache for tick sizes to avoid repeated API calls
/// Tick sizes rarely change, so we cache them with a TTL
#[derive(Debug)]
pub struct TickSizeCache {
    cache: RwLock<HashMap<String, (Decimal, Instant)>>,
    ttl: Duration,
}

impl Default for TickSizeCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(300)) // 5 minute TTL
    }
}

impl TickSizeCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Get cached tick size or None if not cached/expired
    pub fn get(&self, token_id: &str) -> Option<Decimal> {
        let cache = self.cache.read();
        if let Some((tick_size, inserted_at)) = cache.get(token_id) {
            if inserted_at.elapsed() < self.ttl {
                return Some(*tick_size);
            }
        }
        None
    }

    /// Insert a tick size into the cache
    pub fn insert(&self, token_id: String, tick_size: Decimal) {
        let mut cache = self.cache.write();
        cache.insert(token_id, (tick_size, Instant::now()));
    }

    /// Get tick size, fetching from API if not cached
    pub async fn get_or_fetch(
        &self,
        client: &AuthenticatedClient,
        token_id: &str,
    ) -> anyhow::Result<Decimal> {
        // Check cache first
        if let Some(tick_size) = self.get(token_id) {
            debug!(token_id = %token_id, tick_size = %tick_size, "Tick size cache hit");
            return Ok(tick_size);
        }

        // Fetch from API
        let resp = client.tick_size(token_id).await?;
        let tick_size = resp.minimum_tick_size.as_decimal();
        
        debug!(token_id = %token_id, tick_size = %tick_size, "Tick size fetched and cached");
        self.insert(token_id.to_string(), tick_size);
        
        Ok(tick_size)
    }

    /// Prefetch tick sizes for multiple tokens
    pub async fn prefetch(
        &self,
        client: &AuthenticatedClient,
        token_ids: &[String],
    ) {
        for token_id in token_ids {
            if self.get(token_id).is_none() {
                if let Ok(resp) = client.tick_size(token_id).await {
                    self.insert(token_id.clone(), resp.minimum_tick_size.as_decimal());
                }
            }
        }
    }
}

/// Shared tick size cache wrapped in Arc for multi-threaded use
pub type SharedTickSizeCache = Arc<TickSizeCache>;

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
