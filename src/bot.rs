use crate::chainlink::ChainlinkClient;
use crate::config::{ASSETS_BY_NAME, Config};
use crate::edge::{ArbDetector, BuyReason, BuySignal, MarketSnapshot, Side};
use crate::market_cache::MarketCache;
use crate::models::{MarketInfo, MarketState, TradingPair};
use crate::ws::ClobFeed;
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use chrono::{DateTime, Utc};
use chrono_tz::America::New_York;
use parking_lot::RwLock;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::clob::types::Side as ClobSide;
use polymarket_client_sdk::clob::{Client, Config as ClobConfig};
use polymarket_client_sdk::types::Decimal as PolyDecimal;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

type AuthenticatedClient = Client<Authenticated<Normal>>;

#[derive(Debug, Clone)]
pub struct ArbPosition {
    pub market_id: String,
    pub asset: String,
    pub first_side: Side,
    pub first_price: Decimal,
    pub first_order_id: Option<String>,
    pub second_side: Option<Side>,
    pub second_price: Option<Decimal>,
    pub second_order_id: Option<String>,
    pub size: Decimal,
    pub created_at: DateTime<Utc>,
}

impl ArbPosition {
    pub fn is_complete(&self) -> bool {
        self.second_side.is_some()
    }
    pub fn total_cost(&self) -> Decimal {
        self.first_price + self.second_price.unwrap_or(dec!(0))
    }
    pub fn profit(&self) -> Option<Decimal> {
        self.second_price.map(|_| dec!(1) - self.total_cost())
    }
}

pub struct HighFreqArbBot {
    config: Config,
    client: Arc<AuthenticatedClient>,
    signer: PrivateKeySigner,
    market_cache: MarketCache,
    chainlink: ChainlinkClient,
    arb_detector: ArbDetector,
    clob_feed: ClobFeed,
    markets: HashMap<String, MarketState>,
    trading_pairs: HashMap<String, Arc<RwLock<TradingPair>>>,
    positions: HashMap<String, ArbPosition>,
    current_window_end: Option<DateTime<Utc>>,
    last_status_print: Instant,
    last_chainlink_update: Instant,
    chainlink_prices: HashMap<String, Decimal>,
}

impl HighFreqArbBot {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let market_cache = MarketCache::new(config.target_assets.clone());
        let clob_feed = ClobFeed::new();
        let signer = PrivateKeySigner::from_str(&config.polymarket_private_key)?;
        let client = Client::new(crate::config::POLYMARKET_CLOB_HOST, ClobConfig::default())?
            .authentication_builder(&signer.clone().with_chain_id(Some(POLYGON)))
            .authenticate()
            .await?;

        Ok(Self {
            config,
            client: Arc::new(client),
            signer,
            market_cache,
            chainlink: ChainlinkClient::new(),
            arb_detector: ArbDetector::default(),
            clob_feed,
            markets: HashMap::new(),
            trading_pairs: HashMap::new(),
            positions: HashMap::new(),
            current_window_end: None,
            last_status_print: Instant::now(),
            last_chainlink_update: Instant::now(),
            chainlink_prices: HashMap::new(),
        })
    }

    pub async fn run(&mut self) {
        info!("Bot running - One market per asset");
        // Discover the initial markets and register pairs before starting the feed.
        self.discover_markets().await;
        self.clob_feed.set_pairs(self.get_trading_pairs());
        self.clob_feed.start();

        loop {
            let now = Utc::now();
            if self.should_rotate_markets(now) {
                self.positions.clear();
                self.discover_markets().await;
                self.clob_feed.set_pairs(self.get_trading_pairs()); // Ensure WS resubscribes!
                continue;
            }
            if self.last_chainlink_update.elapsed() > Duration::from_secs(1) {
                self.update_prices().await;
                self.last_chainlink_update = Instant::now();
            }
            if self.last_status_print.elapsed() > Duration::from_secs(5) {
                self.print_status();
                self.last_status_print = Instant::now();
            }
            self.reconcile().await;
            self.scan().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn reconcile(&mut self) {
        // Mopping / emergency mop logic disabled for now.
    }

    async fn scan(&mut self) {
        let mut sigs = Vec::new();
        for (m_id, state) in &self.markets {
            if let Some(snap) = self.build_snap(m_id, state) {
                if let Some(pos) = self.positions.get(m_id) {
                    if !pos.is_complete() {
                        if let Some(s) = self.arb_detector.check_second_leg(
                            &snap,
                            pos.first_side,
                            pos.first_price,
                        ) {
                            sigs.push((Some(m_id.clone()), s));
                        }
                    }
                } else if let Some(s) = self.arb_detector.check_first_leg(&snap) {
                    sigs.push((None, s));
                }
            }
        }
        for (m_id, s) in sigs {
            match m_id {
                Some(id) => self.execute_second(&id, &s).await,
                None => self.execute_first(&s).await,
            }
        }
    }

    async fn execute_first(&mut self, sig: &BuySignal) {
        let tid = {
            let s = self.markets.get(&sig.market_id).unwrap();
            if sig.side == Side::Up {
                s.info.up_token_id.clone()
            } else {
                s.info.down_token_id.clone()
            }
        };
        let limit = sig.price - dec!(0.005);
        let oid = if self.config.dry_run {
            None
        } else {
            self.place_order(&tid, limit).await.ok()
        };
        self.positions.insert(
            sig.market_id.clone(),
            ArbPosition {
                market_id: sig.market_id.clone(),
                asset: sig.asset.clone(),
                first_side: sig.side,
                first_price: limit,
                first_order_id: oid,
                second_side: None,
                second_price: None,
                second_order_id: None,
                size: self.config.arb_config.shares_per_side,
                created_at: Utc::now(),
            },
        );
    }

    async fn execute_second(&mut self, m_id: &str, sig: &BuySignal) {
        let (tid, size) = {
            let s = self.markets.get(m_id).unwrap();
            let p = self.positions.get(m_id).unwrap();
            let id = if sig.side == Side::Up {
                s.info.up_token_id.clone()
            } else {
                s.info.down_token_id.clone()
            };
            (id, p.size)
        };
        // Use a taker buffer to ensure fill on the hedge
        let limit = if sig.reason == BuyReason::EmergencyMop {
            sig.price + dec!(0.01)
        } else {
            sig.price - dec!(0.005)
        };
        let oid = if self.config.dry_run {
            None
        } else {
            self.place_order(&tid, limit).await.ok()
        };
        if let Some(p) = self.positions.get_mut(m_id) {
            p.second_side = Some(sig.side);
            p.second_price = Some(limit);
            p.second_order_id = oid;
        }
    }

    async fn place_order(&self, tid: &str, p: Decimal) -> anyhow::Result<String> {
        let pp = PolyDecimal::try_from(p.to_string().as_str()).unwrap();
        let ss = PolyDecimal::try_from(self.config.arb_config.shares_per_side.to_string().as_str())
            .unwrap();
        let signable = self
            .client
            .limit_order()
            .token_id(tid)
            .price(pp)
            .size(ss)
            .side(ClobSide::Buy)
            .build()
            .await?;
        let resp = self
            .client
            .post_order(
                self.client
                    .sign(&self.signer.clone().with_chain_id(Some(POLYGON)), signable)
                    .await?,
            )
            .await?;
        Ok(resp.order_id)
    }

    fn build_snap(&self, m_id: &str, s: &MarketState) -> Option<MarketSnapshot> {
        let open = match s.binance_open_price {
            Some(o) => o,
            None => {
                tracing::info!("Snapshot SKIP {}: no open price", m_id);
                return None;
            }
        };
        let cur = match self.chainlink_prices.get(&s.info.asset).copied() {
            Some(c) => c,
            None => {
                tracing::info!("Snapshot SKIP {}: no chainlink price for {}", m_id, s.info.asset);
                return None;
            }
        };
        let p = s.pair.read();
        let up_ask = p.latest_up_ask();
        let down_ask = p.latest_down_ask();
        if up_ask.is_none() || down_ask.is_none() {
            tracing::info!("Snapshot SKIP {}: missing PM asks up={:?} down={:?}", m_id, up_ask, down_ask);
            return None;
        }
        Some(MarketSnapshot {
            asset: s.info.asset.clone(),
            market_id: m_id.to_string(),
            open_price: open,
            current_price: cur,
            pm_up_ask: up_ask?,
            pm_down_ask: down_ask?,
            elapsed_pct: s.elapsed_pct(Utc::now()),
        })
    }

    fn print_status(&self) {
        let now = Utc::now();
        println!("\n{}", "=".repeat(105));
        println!(
            "STATUS @ {} (EST) | Async Logic: Active",
            now.with_timezone(&New_York).format("%H:%M:%S")
        );
        println!(
            "{:<10} | {:>6} | {:>10} | {:>9} {:>9} | {:>9} {:>9} | {}",
            "ASSET", "ELAP", "MOVE_BPS", "PM_WS_UP", "PM_RS_UP", "PM_WS_DN", "PM_RS_DN", "POSITION"
        );
        println!("{}", "-".repeat(105));
        for (m_id, state) in &self.markets {
            let p = state.pair.read();
            let cur = self
                .chainlink_prices
                .get(&state.info.asset)
                .copied()
                .unwrap_or(dec!(0));
            let (bps_str, open_str) = match state.binance_open_price {
                Some(open) if !open.is_zero() => {
                    let bps = ((cur - open) / open) * dec!(10000);
                    (format!("{:>+10.1}", bps), format!("{:.2}", open))
                }
                _ => ("       --- ".into(), "---".into()),
            };
            let pos = match self.positions.get(m_id) {
                Some(p) if p.is_complete() => format!(
                    "[NEUTRAL cost={:.3} profit={:+.3}]",
                    p.total_cost(),
                    p.profit().unwrap_or(dec!(0))
                ),
                Some(p) => format!(
                    "[ASYNC {:?}@{:.3} age={}s]",
                    p.first_side,
                    p.first_price,
                    (now - p.created_at).num_seconds()
                ),
                None => "[IDLE]".into(),
            };
            println!(
                "{:<10} | {:>5.1}% | {} | {:>7.3} {:>7.3} | {:>7.3} {:>7.3} | {}",
                state.info.asset.to_uppercase(),
                state.elapsed_pct(now) * 100.0,
                bps_str,
                p.ws_up_ask.unwrap_or(dec!(0)),
                p.rest_up_ask.unwrap_or(dec!(0)),
                p.ws_down_ask.unwrap_or(dec!(0)),
                p.rest_down_ask.unwrap_or(dec!(0)),
                pos
            );
        }
        println!("{}", "=".repeat(70));
    }

    pub async fn discover_markets(&mut self) {
        let all = self
            .market_cache
            .get_markets(Utc::now())
            .await
            .unwrap_or_default();
        let mut selected = HashMap::new();
        for m in all.into_iter().filter(|m| m.end_time > Utc::now()) {
            selected.entry(m.asset.clone()).or_insert(m);
        }
        self.markets.clear();
        self.trading_pairs.clear();
        for (asset, info) in selected {
            let m_id = info.id.clone();
            let pair = Arc::new(RwLock::new(info.to_trading_pair()));
            
            // Use the current time to fetch the latest price as the "open price" baseline
            let open_time = Utc::now();
            
            // Fetch open price with retry logic
            let mut open: Option<Decimal> = None;
            for attempt in 0..3 {
                let result = tokio::time::timeout(
                    Duration::from_secs(10),
                    self.chainlink.get_price_at(&asset, open_time),
                )
                .await;
                
                match result {
                    Ok(Ok(price_data)) => {
                        open = Some(price_data.price);
                        tracing::info!("Fetched open price for {}: {}", asset, price_data.price);
                        break;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("Open price fetch for {} attempt {}: {}", asset, attempt + 1, e);
                    }
                    Err(_timeout) => {
                        tracing::warn!("Open price fetch for {} attempt {} timed out", asset, attempt + 1);
                    }
                }
                
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            
            if open.is_none() {
                tracing::warn!("Failed to fetch open price for {} after 3 attempts; will retry in update loop", asset);
            }
            
            self.markets.insert(
                m_id.clone(),
                MarketState {
                    pair: pair.clone(),
                    info: info.clone(),
                    binance_symbol: "".into(),
                    start_time: open_time,
                    end_time: open_time + Duration::from_secs(900),
                    binance_open_price: open,
                },
            );
            self.trading_pairs.insert(m_id, pair);
        }
        self.current_window_end = self.markets.values().map(|s| s.end_time).min();
    }

    async fn update_prices(&mut self) {
        for state in self.markets.values() {
            if let Ok(pd) = self.chainlink.get_latest_price(&state.info.asset).await {
                self.chainlink_prices
                    .insert(state.info.asset.clone(), pd.price);
            }
        }
    }
    fn should_rotate_markets(&self, now: DateTime<Utc>) -> bool {
        self.current_window_end.map_or(true, |end| now >= end)
    }
    fn get_trading_pairs(&self) -> Vec<Arc<RwLock<TradingPair>>> {
        self.trading_pairs.values().cloned().collect()
    }
    pub fn markets(&self) -> &HashMap<String, MarketState> {
        &self.markets
    }
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }
}
