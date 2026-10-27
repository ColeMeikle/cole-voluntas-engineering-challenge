use std::collections::{BTreeMap, HashSet};

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use serde_json::Value;
use tokio_tungstenite::tungstenite::http::Request;

#[derive(Debug)]
pub(crate) enum MarketEvent {
    Reset {
        source_id: usize,
        symbols: Vec<String>,
    },
    Snapshot {
        source_id: usize,
        clock: u64,
        symbol: String,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        bids: BTreeMap<i64, i64>,
        asks: BTreeMap<i64, i64>,
    },
    Delta {
        source_id: usize,
        clock: u64,
        update: PriceLevelUpdateDomain,
    },
}

#[async_trait]
pub(crate) trait ExchangeAdapter: Send + Sync {
    fn name(&self) -> &'static str;

    async fn discover_markets(&self, client: &reqwest::Client) -> Result<HashSet<String>>;

    fn websocket_request(&self) -> Result<Request<()>>;

    fn subscribe_message(&self, command_id: u64, markets: &[String]) -> String;

    fn health_snapshot_message(
        &self,
        command_id: u64,
        subscription_id: u64,
        markets: &[String],
    ) -> String;

    fn resubscribe_messages(
        &self,
        first_command_id: u64,
        subscription_id: u64,
        markets: &[String],
    ) -> Vec<String>;

    fn sequence_number(&self, message: &Value) -> Option<u64>;

    fn subscription_id(&self, message: &Value) -> Option<u64>;

    fn error_message(&self, message: &Value) -> Option<String>;

    fn decode_message(&self, message: &Value, source_id: usize) -> Result<Option<MarketEvent>>;
}
