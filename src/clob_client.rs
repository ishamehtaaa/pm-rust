
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::request::PriceRequest;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum ClobError {
    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Order placement failed: {0}")]
    OrderFailed(String),

    #[error("Cancel failed: {0}")]
    CancelFailed(String),

    #[error("Invalid decimal conversion: {0}")]
    DecimalConversion(String),

    #[error("SDK error: {0}")]
    Sdk(String),
}

#[derive(Debug, Clone)]
pub struct OrderPlacement {
    pub token_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub order_id: Option<String>,
    pub error: Option<String>,
}

impl OrderPlacement {
    pub fn placed(&self) -> bool {
        self.order_id.is_some()
    }

    pub fn not_placed(&self) -> bool {
        self.order_id.is_none()
    }

    pub fn dry_run(token_id: String, price: Decimal, size: Decimal) -> Self {
        Self {
            token_id,
            price,
            size,
            order_id: Some(format!("dry-{}", crate::models::now_ms())),
            error: None,
        }
    }

    pub fn failed(token_id: String, price: Decimal, size: Decimal, error: String) -> Self {
        Self {
            token_id,
            price,
            size,
            order_id: None,
            error: Some(error),
        }
    }

    pub fn success(token_id: String, price: Decimal, size: Decimal, order_id: String) -> Self {
        Self {
            token_id,
            price,
            size,
            order_id: Some(order_id),
            error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderInfo {
    pub id: String,
    pub status: String,
    pub size_matched: Option<Decimal>,
}

#[derive(Debug, Clone, Copy)]
pub enum PriceSide {
    Buy,
    Sell,
}

impl From<PriceSide> for ClobSide {
    fn from(side: PriceSide) -> Self {
        match side {
            PriceSide::Buy => ClobSide::Buy,
            PriceSide::Sell => ClobSide::Sell,
        }
    }
}

type AuthenticatedClient = Client<Authenticated<Normal>>;

pub struct ClobClient {
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    dry_run: bool,
}

impl ClobClient {
    pub async fn new(private_key: &str, dry_run: bool) -> Result<Self, ClobError> {
        let signer = PrivateKeySigner::from_str(private_key)
            .map_err(|e| ClobError::Auth(format!("Invalid private key: {}", e)))?;
        
        let address = signer.address();
        info!("Wallet address: {}", address);
        
        let signer_with_chain = signer.clone().with_chain_id(Some(POLYGON));

        let client = Client::new(crate::config::POLYMARKET_CLOB_HOST, Config::default())
            .map_err(|e| ClobError::Sdk(e.to_string()))?
            .authentication_builder(&signer_with_chain)
            .authenticate()
            .await
            .map_err(|e| ClobError::Auth(e.to_string()))?;

        info!("CLOB client authenticated (dry_run={})", dry_run);

        Ok(Self {
            client: Arc::new(client),
            signer,
            dry_run,
        })
    }

    pub async fn place_limit_order(
        &self,
        token_id: &str,
        price: Decimal,
        size: Decimal,
    ) -> OrderPlacement {
        let total_cost = price * size;
        
        info!(
            "ORDER: token={} price={} size={} total_cost={}",
            token_id, price, size, total_cost
        );

        if self.dry_run {
            info!("DRY RUN: would place order");
            return OrderPlacement::dry_run(token_id.to_string(), price, size);
        }

        let start = Instant::now();

        let poly_price = match to_poly_decimal(price) {
            Ok(p) => p,
            Err(e) => {
                warn!("Price conversion failed: {}", e);
                return OrderPlacement::failed(
                    token_id.to_string(),
                    price,
                    size,
                    format!("price conversion: {}", e),
                )
            }
        };

        let poly_size = match to_poly_decimal(size) {
            Ok(s) => s,
            Err(e) => {
                warn!("Size conversion failed: {}", e);
                return OrderPlacement::failed(
                    token_id.to_string(),
                    price,
                    size,
                    format!("size conversion: {}", e),
                )
            }
        };

        info!(
            "SDK params: poly_price={} poly_size={} side=Buy",
            poly_price, poly_size
        );

        // Build limit order using SDK builder
        let signable_order = match self
            .client
            .limit_order()
            .token_id(token_id)
            .price(poly_price)
            .size(poly_size)
            .side(ClobSide::Buy)
            .build()
            .await
        {
            Ok(o) => {
                debug!("Order built successfully");
                o
            }
            Err(e) => {
                warn!("Failed to build order: {}", e);
                return OrderPlacement::failed(
                    token_id.to_string(),
                    price,
                    size,
                    format!("build: {}", e),
                );
            }
        };

        // Sign the order
        let signer_with_chain = self.signer.clone().with_chain_id(Some(POLYGON));
        let signed_order = match self.client.sign(&signer_with_chain, signable_order).await {
            Ok(s) => {
                debug!("Order signed successfully");
                s
            }
            Err(e) => {
                warn!("Failed to sign order: {}", e);
                return OrderPlacement::failed(
                    token_id.to_string(),
                    price,
                    size,
                    format!("sign: {}", e),
                );
            }
        };

        // Post to exchange
        let response = match self.client.post_order(signed_order).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to post order: {}", e);
                return OrderPlacement::failed(
                    token_id.to_string(),
                    price,
                    size,
                    format!("post: {}", e),
                );
            }
        };

        let elapsed = start.elapsed();
        info!(
            "Order posted in {:?}: order_id={}",
            elapsed, response.order_id
        );

        OrderPlacement::success(token_id.to_string(), price, size, response.order_id)
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<(), ClobError> {
        if self.dry_run {
            debug!("DRY RUN: cancel order {}", order_id);
            return Ok(());
        }

        let _response = self.client
            .cancel_order(order_id)
            .await
            .map_err(|e: polymarket_client_sdk::error::Error| ClobError::CancelFailed(e.to_string()))?;

        debug!("Cancelled order {}", order_id);
        Ok(())
    }

    pub async fn cancel_all(&self) -> Result<(), ClobError> {
        if self.dry_run {
            debug!("DRY RUN: cancel all orders");
            return Ok(());
        }

        let _response = self.client
            .cancel_all_orders()
            .await
            .map_err(|e: polymarket_client_sdk::error::Error| ClobError::CancelFailed(e.to_string()))?;

        info!("Cancelled all orders");
        Ok(())
    }

    pub async fn get_order(&self, order_id: &str) -> Result<OrderInfo, ClobError> {
        if self.dry_run && order_id.starts_with("dry-") {
            return Ok(OrderInfo {
                id: order_id.to_string(),
                status: "MATCHED".to_string(),
                size_matched: None,
            });
        }

        let order = self
            .client
            .order(order_id)
            .await
            .map_err(|e: polymarket_client_sdk::error::Error| ClobError::Sdk(e.to_string()))?;

        let size_matched = Decimal::from_str(&order.size_matched.to_string()).ok();

        Ok(OrderInfo {
            id: order.id,
            status: order.status.to_string(),
            size_matched,
        })
    }

    pub async fn get_best_price(
        &self,
        token_id: &str,
        side: PriceSide,
    ) -> Result<Decimal, ClobError> {
        let request = PriceRequest::builder()
            .token_id(token_id)
            .side(side.into())
            .build();

        let response = self
            .client
            .price(&request)
            .await
            .map_err(|e| ClobError::Sdk(e.to_string()))?;

        from_poly_decimal(&response.price)
    }
}

fn to_poly_decimal(d: Decimal) -> Result<PolyDecimal, ClobError> {
    PolyDecimal::try_from(d.to_string().as_str())
        .map_err(|e| ClobError::DecimalConversion(format!("{:?}", e)))
}

fn from_poly_decimal(d: &PolyDecimal) -> Result<Decimal, ClobError> {
    Decimal::from_str(&d.to_string()).map_err(|e| ClobError::DecimalConversion(e.to_string()))
}