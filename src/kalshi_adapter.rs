use std::{
    collections::{BTreeMap, HashSet},
    env, fs,
};

use anyhow::{Context, bail};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};
use pkcs8::DecodePrivateKey;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderValue, Request},
};

use engineering_challenge::{Side, util::dtos::challenge_orderbook::PriceLevelUpdateDomain};

use crate::exchange::{ExchangeAdapter, MarketEvent};

const WS_PATH: &str = "/trade-api/ws/v2";

pub(crate) struct KalshiAdapter {
    api_key: String,
    signing_key: SigningKey,
    rest_url: String,
    websocket_url: String,
}

#[derive(Deserialize)]
struct MarketsResponse {
    #[serde(default)]
    markets: Vec<Market>,
    #[serde(default)]
    cursor: String,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
}

impl KalshiAdapter {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let environment = env::var("KALSHI_ENV").unwrap_or_else(|_| "production".to_owned());
        let (default_rest_url, default_websocket_url) = endpoints_for_environment(&environment)?;
        let api_key = env::var("KALSHI_ACCESS_KEY")
            .context("KALSHI_ACCESS_KEY is required (Kalshi API key ID)")?;
        anyhow::ensure!(
            !api_key.trim().is_empty() && !api_key.contains("PASTE_YOUR"),
            "set KALSHI_ACCESS_KEY to your Kalshi API key ID"
        );
        let private_key_path = env::var("KALSHI_PRIVATE_KEY_PATH")
            .context("KALSHI_PRIVATE_KEY_PATH must point to your private-key PEM file")?;
        let private_key = fs::read_to_string(&private_key_path)
            .with_context(|| format!("reading Kalshi private key from {private_key_path}"))?;
        let signing_key = SigningKey::from_pkcs8_pem(&private_key)
            .context("parsing Kalshi Ed25519 PKCS#8 private key")?;
        Ok(Self {
            api_key,
            signing_key,
            rest_url: env::var("KALSHI_REST_URL").unwrap_or_else(|_| default_rest_url.to_owned()),
            websocket_url: env::var("KALSHI_WEBSOCKET_URL")
                .unwrap_or_else(|_| default_websocket_url.to_owned()),
        })
    }

    fn signed_headers(&self, method: &str, path: &str) -> anyhow::Result<(String, String)> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let message = format!("{timestamp}{method}{path}");
        let signature = self.signing_key.sign(message.as_bytes());
        Ok((timestamp, STANDARD.encode(signature.to_bytes())))
    }

    fn authenticated_request(&self, method: &str, path: &str) -> anyhow::Result<Request<()>> {
        let (timestamp, signature) = self.signed_headers(method, path)?;
        let mut request = self
            .websocket_url
            .as_str()
            .into_client_request()
            .context("building Kalshi WebSocket handshake request")?;
        *request.method_mut() = method
            .parse()
            .context("setting Kalshi WebSocket handshake method")?;
        request
            .headers_mut()
            .insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&self.api_key)?);
        request.headers_mut().insert(
            "KALSHI-ACCESS-SIGNATURE",
            HeaderValue::from_str(&signature)?,
        );
        request.headers_mut().insert(
            "KALSHI-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp)?,
        );
        Ok(request)
    }
}

fn endpoints_for_environment(environment: &str) -> anyhow::Result<(&'static str, &'static str)> {
    match environment.trim().to_ascii_lowercase().as_str() {
        "demo" => Ok((
            "https://external-api.demo.kalshi.co/trade-api/v2",
            "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2",
        )),
        "prod" | "production" => Ok((
            "https://external-api.kalshi.com/trade-api/v2",
            "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        )),
        _ => bail!("KALSHI_ENV must be 'demo', 'prod', or 'production'"),
    }
}

#[async_trait]
impl ExchangeAdapter for KalshiAdapter {
    fn name(&self) -> &'static str {
        "Kalshi"
    }

    async fn discover_markets(&self, client: &reqwest::Client) -> anyhow::Result<HashSet<String>> {
        let mut markets = HashSet::new();
        let mut cursor = String::new();
        loop {
            let mut request = client
                .get(format!("{}/markets", self.rest_url.trim_end_matches('/')))
                .query(&[
                    ("status", "open"),
                    ("mve_filter", "exclude"),
                    ("limit", "1000"),
                ]);
            if !cursor.is_empty() {
                request = request.query(&[("cursor", cursor.as_str())]);
            }
            let page: MarketsResponse = request
                .send()
                .await
                .context("requesting open Kalshi markets")?
                .error_for_status()
                .context("Kalshi market discovery returned an HTTP error")?
                .json()
                .await
                .context("decoding Kalshi market discovery response")?;
            markets.extend(page.markets.into_iter().map(|market| market.ticker));
            if page.cursor.is_empty() || page.cursor == cursor {
                break;
            }
            cursor = page.cursor;
        }
        Ok(markets)
    }

    fn websocket_request(&self) -> anyhow::Result<Request<()>> {
        self.authenticated_request("GET", WS_PATH)
    }

    fn subscribe_message(&self, command_id: u64, markets: &[String]) -> String {
        json!({
            "id": command_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": markets,
                "use_yes_price": true
            }
        })
        .to_string()
    }

    fn health_snapshot_message(
        &self,
        command_id: u64,
        subscription_id: u64,
        markets: &[String],
    ) -> String {
        json!({
            "id": command_id,
            "cmd": "update_subscription",
            "params": {
                "sids": [subscription_id],
                "action": "get_snapshot",
                "market_tickers": markets
            }
        })
        .to_string()
    }

    fn resubscribe_messages(
        &self,
        first_command_id: u64,
        subscription_id: u64,
        markets: &[String],
    ) -> Vec<String> {
        ["delete_markets", "add_markets"]
            .into_iter()
            .enumerate()
            .map(|(index, action)| {
                json!({
                    "id": first_command_id.wrapping_add(index as u64),
                    "cmd": "update_subscription",
                    "params": {
                        "sids": [subscription_id],
                        "action": action,
                        "market_tickers": markets
                    }
                })
                .to_string()
            })
            .collect()
    }

    fn sequence_number(&self, message: &Value) -> Option<u64> {
        message.get("seq").and_then(Value::as_u64)
    }

    fn subscription_id(&self, message: &Value) -> Option<u64> {
        message.get("sid").and_then(Value::as_u64)
    }

    fn error_message(&self, message: &Value) -> Option<String> {
        (message.get("type").and_then(Value::as_str) == Some("error")).then(|| {
            message
                .get("msg")
                .cloned()
                .unwrap_or(Value::Null)
                .to_string()
        })
    }

    fn decode_message(
        &self,
        value: &Value,
        source_id: usize,
    ) -> anyhow::Result<Option<MarketEvent>> {
        let Some(kind) = value.get("type").and_then(Value::as_str) else {
            return Ok(None);
        };
        if kind != "orderbook_snapshot" && kind != "orderbook_delta" {
            return Ok(None);
        }
        let Some(message) = value.get("msg") else {
            return Ok(None);
        };
        let symbol = message
            .get("market_ticker")
            .and_then(Value::as_str)
            .context("market_ticker is missing")?
            .to_owned();
        let received_at = Utc::now();
        let clock = message
            .get("clock")
            .or_else(|| value.get("clock"))
            .and_then(|clock| {
                clock
                    .as_u64()
                    .or_else(|| clock.as_str().and_then(|text| text.parse().ok()))
            })
            .or_else(|| (kind == "orderbook_snapshot").then_some(0))
            .with_context(|| {
                let message_keys = message
                    .as_object()
                    .map(|object| object.keys().map(String::as_str).collect::<Vec<_>>())
                    .unwrap_or_default();
                format!(
                    "{kind} message has no valid exchange clock (expected a non-negative integer in msg.clock; msg fields: {})",
                    message_keys.join(", ")
                )
            })?;
        let sending_timestamp = value
            .get("sending_ts_ms")
            .and_then(Value::as_i64)
            .and_then(DateTime::from_timestamp_millis);
        match kind {
            "orderbook_snapshot" => Ok(Some(MarketEvent::Snapshot {
                source_id,
                clock,
                symbol,
                received_at,
                exchange_timestamp: sending_timestamp.unwrap_or(received_at),
                bids: parse_levels(message.get("yes_dollars_fp"))?,
                asks: parse_levels(message.get("no_dollars_fp"))?,
            })),
            "orderbook_delta" => {
                let price = decimal_to_fixed(
                    message
                        .get("price_dollars")
                        .and_then(Value::as_str)
                        .context("price_dollars is missing")?,
                )?;
                let delta = decimal_to_fixed(
                    message
                        .get("delta_fp")
                        .and_then(Value::as_str)
                        .context("delta_fp is missing")?,
                )?;
                let side = match message
                    .get("side")
                    .and_then(Value::as_str)
                    .context("side is missing")?
                {
                    "yes" => Side::Buy,
                    "no" => Side::Sell,
                    invalid => bail!("unsupported market side {invalid:?}"),
                };
                let exchange_timestamp = message
                    .get("ts_ms")
                    .and_then(Value::as_i64)
                    .and_then(DateTime::from_timestamp_millis)
                    .or_else(|| {
                        message
                            .get("ts")
                            .and_then(Value::as_str)
                            .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                            .map(|ts| ts.with_timezone(&Utc))
                    })
                    .or(sending_timestamp)
                    .unwrap_or(received_at);
                Ok(Some(MarketEvent::Delta {
                    source_id,
                    clock,
                    update: PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp,
                        price_level: price,
                        quantity: delta,
                        side,
                        symbol,
                    },
                }))
            }
            _ => Ok(None),
        }
    }
}

fn parse_levels(value: Option<&Value>) -> anyhow::Result<BTreeMap<i64, i64>> {
    let mut levels = BTreeMap::new();
    let Some(value) = value else {
        return Ok(levels);
    };
    for level in value
        .as_array()
        .context("orderbook price levels must be an array")?
    {
        let pair = level
            .as_array()
            .context("each price level must be a two-element array")?;
        anyhow::ensure!(
            pair.len() == 2,
            "each price level must contain price and quantity"
        );
        let price = decimal_to_fixed(
            pair[0]
                .as_str()
                .context("orderbook price must be a string")?,
        )?;
        let quantity = decimal_to_fixed(
            pair[1]
                .as_str()
                .context("orderbook quantity must be a string")?,
        )?;
        anyhow::ensure!(
            price >= 0 && quantity > 0,
            "orderbook snapshot contains an invalid price or quantity"
        );
        levels.insert(price, quantity);
    }
    Ok(levels)
}

fn decimal_to_fixed(value: &str) -> anyhow::Result<i64> {
    let (negative, unsigned) = match value.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let mut parts = unsigned.split('.');
    let whole = parts
        .next()
        .context("decimal value is empty")?
        .parse::<i64>()
        .with_context(|| format!("invalid decimal value {value:?}"))?;
    let fraction = parts.next().unwrap_or("");
    anyhow::ensure!(
        parts.next().is_none()
            && fraction.len() <= 6
            && fraction.bytes().all(|c| c.is_ascii_digit()),
        "decimal value {value:?} has invalid precision"
    );
    let scale = 10_i64
        .checked_pow((6 - fraction.len()) as u32)
        .context("decimal scale overflow")?;
    let fractional = if fraction.is_empty() {
        0
    } else {
        fraction
            .parse::<i64>()
            .with_context(|| format!("invalid decimal value {value:?}"))?
            .checked_mul(scale)
            .context("decimal fraction overflow")?
    };
    let fixed = whole
        .checked_mul(1_000_000)
        .and_then(|whole| whole.checked_add(fractional))
        .context("decimal value overflow")?;
    if negative {
        fixed.checked_neg().context("decimal value overflow")
    } else {
        Ok(fixed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_environment_accepts_prod_alias() {
        assert_eq!(
            endpoints_for_environment("prod").unwrap(),
            (
                "https://external-api.kalshi.com/trade-api/v2",
                "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
            )
        );
        assert_eq!(
            endpoints_for_environment("production").unwrap(),
            endpoints_for_environment("prod").unwrap()
        );
        assert!(endpoints_for_environment("staging").is_err());
    }

    fn adapter() -> KalshiAdapter {
        KalshiAdapter {
            api_key: String::new(),
            signing_key: SigningKey::from_bytes(&[1; 32]),
            rest_url: String::new(),
            websocket_url: String::new(),
        }
    }

    #[test]
    fn command_encoders_include_symbols_and_subscription_ids() {
        let adapter = adapter();
        let markets = vec!["MARKET-A".to_owned()];
        let subscribe: Value =
            serde_json::from_str(&adapter.subscribe_message(7, &markets)).unwrap();
        assert_eq!(subscribe["cmd"], "subscribe");
        assert_eq!(subscribe["params"]["market_tickers"][0], "MARKET-A");
        assert_eq!(subscribe["params"]["use_yes_price"], true);

        let health: Value =
            serde_json::from_str(&adapter.health_snapshot_message(8, 12, &markets)).unwrap();
        assert_eq!(health["params"]["sids"][0], 12);
        assert_eq!(health["params"]["action"], "get_snapshot");

        let resubscribe = adapter.resubscribe_messages(9, 12, &markets);
        let delete: Value = serde_json::from_str(&resubscribe[0]).unwrap();
        let add: Value = serde_json::from_str(&resubscribe[1]).unwrap();
        assert_eq!(delete["params"]["sids"][0], 12);
        assert_eq!(delete["params"]["action"], "delete_markets");
        assert_eq!(add["params"]["action"], "add_markets");
    }

    #[test]
    fn authenticated_websocket_request_contains_required_handshake_headers() {
        let mut adapter = adapter();
        adapter.api_key = "test-key-id".to_owned();
        adapter.websocket_url = "wss://example.test/trade-api/ws/v2".to_owned();
        let request = adapter.websocket_request().unwrap();
        assert_eq!(request.method(), "GET");
        assert_eq!(request.uri().path(), "/trade-api/ws/v2");
        assert!(request.headers().contains_key("sec-websocket-key"));
        assert!(request.headers().contains_key("sec-websocket-version"));
        assert_eq!(request.headers()["KALSHI-ACCESS-KEY"], "test-key-id");
        assert!(request.headers().contains_key("KALSHI-ACCESS-SIGNATURE"));
        assert!(request.headers().contains_key("KALSHI-ACCESS-TIMESTAMP"));
    }

    #[test]
    fn decodes_delta_and_snapshot_messages() {
        let adapter = adapter();
        let event = adapter
            .decode_message(
                &json!({
                    "type": "orderbook_delta",
                    "sending_ts_ms": 1_669_149_841_123_i64,
                    "seq": 3,
                    "msg": {
                        "market_ticker": "TEST-MARKET",
                        "clock": 7,
                        "price_dollars": "0.9600",
                        "delta_fp": "-54.00",
                        "side": "yes",
                        "ts_ms": 1_669_149_841_000_i64
                    }
                }),
                2,
            )
            .unwrap()
            .unwrap();
        let MarketEvent::Delta {
            source_id,
            clock,
            update,
        } = event
        else {
            panic!("expected delta");
        };
        assert_eq!(source_id, 2);
        assert_eq!(clock, 7);
        assert_eq!(update.price_level, 960_000);
        assert_eq!(update.quantity, -54_000_000);
        assert_eq!(update.side, Side::Buy);
        assert_eq!(
            update.exchange_timestamp.timestamp_millis(),
            1_669_149_841_000
        );

        let snapshot = adapter
            .decode_message(
                &json!({
                    "type": "orderbook_snapshot",
                    "sending_ts_ms": 1_669_149_841_234_i64,
                    "msg": {
                        "market_ticker": "TEST-MARKET",
                        "clock": 5,
                        "yes_dollars_fp": [["0.08", "300.00"]]
                    }
                }),
                1,
            )
            .unwrap()
            .unwrap();
        let MarketEvent::Snapshot { bids, asks, .. } = snapshot else {
            panic!("expected snapshot");
        };
        assert_eq!(bids.get(&80_000), Some(&300_000_000));
        assert!(asks.is_empty());
    }

    #[test]
    fn decodes_exchange_clock_as_number_or_numeric_string() {
        let adapter = adapter();
        for clock in [json!(18), json!("18")] {
            let event = adapter
                .decode_message(
                    &json!({
                        "type": "orderbook_delta",
                        "msg": {
                            "market_ticker": "TEST-MARKET",
                            "clock": clock,
                            "price_dollars": "0.5",
                            "delta_fp": "1",
                            "side": "yes"
                        }
                    }),
                    0,
                )
                .unwrap()
                .unwrap();
            let MarketEvent::Delta { clock, .. } = event else {
                panic!("expected delta");
            };
            assert_eq!(clock, 18);
        }

        let event = adapter
            .decode_message(
                &json!({
                    "type": "orderbook_delta",
                    "clock": 19,
                    "msg": {
                        "market_ticker": "TEST-MARKET",
                        "price_dollars": "0.5",
                        "delta_fp": "1",
                        "side": "yes"
                    }
                }),
                0,
            )
            .unwrap()
            .unwrap();
        let MarketEvent::Delta { clock, .. } = event else {
            panic!("expected delta");
        };
        assert_eq!(clock, 19);
    }

    #[test]
    fn snapshot_without_exchange_clock_uses_initial_clock_zero() {
        let event = adapter()
            .decode_message(
                &json!({
                    "type": "orderbook_snapshot",
                    "msg": {
                        "market_ticker": "TEST-MARKET",
                        "yes_dollars_fp": [],
                        "no_dollars_fp": []
                    }
                }),
                0,
            )
            .unwrap()
            .unwrap();
        let MarketEvent::Snapshot { clock, .. } = event else {
            panic!("expected snapshot");
        };
        assert_eq!(clock, 0);
    }

    #[test]
    fn delta_without_exchange_clock_is_rejected() {
        let error = adapter()
            .decode_message(
                &json!({
                    "type": "orderbook_delta",
                    "msg": {
                        "market_ticker": "TEST-MARKET",
                        "price_dollars": "0.5",
                        "delta_fp": "1",
                        "side": "yes"
                    }
                }),
                0,
            )
            .unwrap_err();
        assert!(error.to_string().contains("orderbook_delta"));
    }

    #[test]
    fn decimal_conversion_uses_six_decimal_fixed_point() {
        assert_eq!(decimal_to_fixed("0.0800").unwrap(), 80_000);
        assert_eq!(decimal_to_fixed("300.00").unwrap(), 300_000_000);
        assert_eq!(decimal_to_fixed("-54.00").unwrap(), -54_000_000);
    }
}
