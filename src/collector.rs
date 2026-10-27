use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WebSocketError, Message},
};

use crate::exchange::{ExchangeAdapter, MarketEvent};
use engineering_challenge::{
    Side,
    util::{
        AlignedSbeWriter,
        dtos::challenge_orderbook::{
            PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
            SnapshotDomain,
        },
    },
};

const DEFAULT_MARKETS_PER_CONNECTION: usize = 500;
const DEFAULT_MAX_CONNECTIONS: usize = 180;
const REDUNDANT_CONNECTIONS_PER_SHARD: usize = 2;
const MARKET_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(60);
const BOOK_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const SOCKET_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const SOCKET_PING_INTERVAL: Duration = Duration::from_secs(15);
const SOCKET_PONG_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_ACTIVITY_WINDOW_SECS: u64 = 60;
const DEFAULT_MIN_UPDATES_PER_WINDOW: u64 = 1;
const DEFAULT_QUIET_RESUBSCRIBE_COOLDOWN_SECS: u64 = 5 * 60;
const DEFAULT_SOCKET_ROTATION_SECS: u64 = 6 * 60 * 60;

#[derive(Clone)]
pub(crate) struct Config {
    markets_per_connection: usize,
    max_connections: usize,
    activity_window: Duration,
    min_updates_per_window: u64,
    quiet_resubscribe_cooldown: Duration,
    socket_rotation: Duration,
    pub(crate) output_path: PathBuf,
}

struct Orderbook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    exchange_timestamp: DateTime<Utc>,
    clock: u64,
}

struct Shard {
    markets: HashSet<String>,
    assignments: watch::Sender<HashSet<String>>,
    task: JoinHandle<()>,
}

impl Config {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
        let markets_per_connection = env::var("COLLECTOR_MARKETS_PER_CONNECTION")
            .or_else(|_| env::var("KALSHI_MARKETS_PER_CONNECTION"))
            .map(|value| {
                value
                    .parse::<usize>()
                    .context("COLLECTOR_MARKETS_PER_CONNECTION must be a positive integer")
            })
            .unwrap_or(Ok(DEFAULT_MARKETS_PER_CONNECTION))?;
        anyhow::ensure!(
            markets_per_connection > 0,
            "COLLECTOR_MARKETS_PER_CONNECTION must be greater than zero"
        );
        let max_connections = env::var("COLLECTOR_MAX_CONNECTIONS")
            .map(|value| {
                value
                    .parse::<usize>()
                    .context("COLLECTOR_MAX_CONNECTIONS must be a positive even integer")
            })
            .unwrap_or(Ok(DEFAULT_MAX_CONNECTIONS))?;
        anyhow::ensure!(
            max_connections >= REDUNDANT_CONNECTIONS_PER_SHARD
                && max_connections % REDUNDANT_CONNECTIONS_PER_SHARD == 0,
            "COLLECTOR_MAX_CONNECTIONS must be a positive even integer of at least 2"
        );
        let activity_window_secs = parse_positive_env(
            "COLLECTOR_ACTIVITY_WINDOW_SECS",
            "KALSHI_ACTIVITY_WINDOW_SECS",
            DEFAULT_ACTIVITY_WINDOW_SECS,
        )?;
        let min_updates_per_window = env::var("COLLECTOR_MIN_UPDATES_PER_WINDOW")
            .or_else(|_| env::var("KALSHI_MIN_UPDATES_PER_WINDOW"))
            .map(|value| {
                value
                    .parse::<u64>()
                    .context("COLLECTOR_MIN_UPDATES_PER_WINDOW must be a non-negative integer")
            })
            .unwrap_or(Ok(DEFAULT_MIN_UPDATES_PER_WINDOW))?;
        let quiet_resubscribe_cooldown = parse_positive_env(
            "COLLECTOR_QUIET_RESUBSCRIBE_COOLDOWN_SECS",
            "KALSHI_QUIET_RESUBSCRIBE_COOLDOWN_SECS",
            DEFAULT_QUIET_RESUBSCRIBE_COOLDOWN_SECS,
        )?;
        let socket_rotation_secs = parse_positive_env(
            "COLLECTOR_SOCKET_ROTATION_SECS",
            "KALSHI_SOCKET_ROTATION_SECS",
            DEFAULT_SOCKET_ROTATION_SECS,
        )?;
        let output_path = env::var_os("ORDERBOOK_OUTPUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("orderbook.sbe"));

        Ok(Self {
            markets_per_connection,
            max_connections,
            activity_window: Duration::from_secs(activity_window_secs),
            min_updates_per_window,
            quiet_resubscribe_cooldown: Duration::from_secs(quiet_resubscribe_cooldown),
            socket_rotation: Duration::from_secs(socket_rotation_secs),
            output_path,
        })
    }
}

fn parse_positive_env(name: &str, legacy_name: &str, default: u64) -> anyhow::Result<u64> {
    let value = env::var(name)
        .or_else(|_| env::var(legacy_name))
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("{name} must be a positive integer"))
        })
        .unwrap_or(Ok(default))?;
    anyhow::ensure!(value > 0, "{name} must be greater than zero");
    Ok(value)
}

struct MarketActivity {
    window_started: Instant,
    updates: u64,
    resubscribe_after: Instant,
}

impl MarketActivity {
    fn new(now: Instant) -> Self {
        Self {
            window_started: now,
            updates: 0,
            resubscribe_after: now,
        }
    }

    fn record(&mut self) {
        self.updates = self.updates.saturating_add(1);
    }

    fn finish_window(&mut self, now: Instant, window: Duration, minimum_updates: u64) -> bool {
        if now.duration_since(self.window_started) < window {
            return false;
        }
        let quiet = self.updates < minimum_updates;
        self.window_started = now;
        self.updates = 0;
        quiet
    }

    fn ready_to_resubscribe(&self, now: Instant) -> bool {
        now >= self.resubscribe_after
    }

    fn mark_resubscribed(&mut self, now: Instant, cooldown: Duration) {
        self.window_started = now;
        self.updates = 0;
        self.resubscribe_after = now + cooldown;
    }
}

pub(crate) async fn run(
    config: Config,
    adapter: Arc<dyn ExchangeAdapter>,
    mut writer: AlignedSbeWriter,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    eprintln!(
        "starting {} collector (SBE output: {})",
        adapter.name(),
        config.output_path.display()
    );
    verify_websocket_auth(adapter.as_ref()).await?;
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(20))
        .build()?;
    let (events_tx, mut events_rx) = mpsc::channel::<MarketEvent>(100_000);
    let manager_config = config.clone();
    let manager_adapter = Arc::clone(&adapter);
    let manager_shutdown = shutdown.clone();
    let manager = tokio::spawn(async move {
        manage_markets(
            manager_config,
            manager_adapter,
            client,
            events_tx,
            manager_shutdown,
        )
        .await
    });

    let mut orderbooks = HashMap::<(usize, String), Orderbook>::new();
    let mut active_sources = HashMap::<String, usize>::new();
    let mut snapshot_timer = time::interval(SNAPSHOT_INTERVAL);
    snapshot_timer.tick().await;
    let outcome = async {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break Ok(());
                    }
                }
                event = events_rx.recv() => {
                    let Some(event) = event else {
                        break Err(anyhow::anyhow!("market feed stopped unexpectedly"));
                    };
                    match event {
                        MarketEvent::Reset { source_id, symbols } => {
                            for symbol in symbols {
                                orderbooks.remove(&(source_id, symbol.clone()));
                                if active_sources.get(&symbol) == Some(&source_id) {
                                    active_sources.remove(&symbol);
                                    if let Some((replacement_source, book)) = orderbooks
                                        .iter()
                                        .filter(|((_, candidate), _)| candidate == &symbol)
                                        .max_by_key(|(_, book)| book.clock)
                                        .map(|((source, _), book)| (*source, book))
                                    {
                                        active_sources.insert(symbol.clone(), replacement_source);
                                        emit_snapshot(
                                            &mut writer,
                                            &symbol,
                                            Utc::now(),
                                            book.exchange_timestamp,
                                            book,
                                        )?;
                                        eprintln!(
                                            "market {symbol}: switched active source to WebSocket {replacement_source} after source {source_id} reset"
                                        );
                                    }
                                }
                            }
                        }
                        MarketEvent::Snapshot {
                            source_id,
                            clock,
                            symbol,
                            received_at,
                            exchange_timestamp,
                            bids,
                            asks,
                        } => {
                            let book = Orderbook {
                                bids,
                                asks,
                                exchange_timestamp,
                                clock,
                            };
                            let key = (source_id, symbol.clone());
                            let old_clock = orderbooks.get(&key).map(|book| book.clock);
                            orderbooks.insert(key, book);
                            let selected_source = active_sources.get(&symbol).copied();
                            let selected_clock = selected_source
                                .and_then(|source| orderbooks.get(&(source, symbol.clone())))
                                .map(|book| book.clock);
                            let should_switch = selected_source == Some(source_id)
                                || should_promote_source(
                                    selected_source,
                                    selected_clock,
                                    source_id,
                                    clock,
                                );
                            if should_switch {
                                let changed_source = selected_source != Some(source_id);
                                active_sources.insert(symbol.clone(), source_id);
                                let changed_clock = old_clock.is_none_or(|old| clock > old);
                                if changed_source || changed_clock {
                                    let book = &orderbooks[&(source_id, symbol.clone())];
                                    emit_snapshot(
                                        &mut writer,
                                        &symbol,
                                        received_at,
                                        exchange_timestamp,
                                        book,
                                    )?;
                                    if changed_source && selected_source.is_some() {
                                        eprintln!(
                                            "market {symbol}: switched active source to WebSocket {source_id} at exchange clock {clock}"
                                        );
                                    }
                                }
                            }
                        }
                        MarketEvent::Delta { source_id, clock, mut update } => {
                            let key = (source_id, update.symbol.clone());
                            let Some(book) = orderbooks.get_mut(&key) else {
                                eprintln!(
                                    "received a delta for {} on WebSocket {source_id} before its snapshot; awaiting resynchronization",
                                    update.symbol,
                                );
                                continue;
                            };
                            apply_delta(book, &mut update)?;
                            book.clock = clock;
                            let selected_source = active_sources.get(&update.symbol).copied();
                            let selected_clock = selected_source
                                .and_then(|source| orderbooks.get(&(source, update.symbol.clone())))
                                .map(|book| book.clock);
                            if should_promote_source(
                                selected_source,
                                selected_clock,
                                source_id,
                                clock,
                            )
                            {
                                active_sources.insert(update.symbol.clone(), source_id);
                                let book = &orderbooks[&(source_id, update.symbol.clone())];
                                emit_snapshot(
                                    &mut writer,
                                    &update.symbol,
                                    update.backtest_timestamp,
                                    update.exchange_timestamp,
                                    book,
                                )?;
                                if selected_source.is_some() && selected_source != Some(source_id) {
                                    eprintln!(
                                        "market {}: switched active source to WebSocket {source_id} at exchange clock {clock}",
                                        update.symbol
                                    );
                                }
                                continue;
                            }
                            if selected_source != Some(source_id) {
                                continue;
                            }
                            let quantity = update.quantity;
                            writer.write_msg(&update)?;
                            println!("{}", json!({
                                "type": "delta",
                                "backtest_timestamp": update.backtest_timestamp.to_rfc3339(),
                                "exchange_timestamp": update.exchange_timestamp.to_rfc3339(),
                                "symbol": update.symbol,
                                "side": update.side.to_string(),
                                "price_level": update.price_level,
                                "quantity": quantity,
                            }));
                        }
                    }
                }
                _ = snapshot_timer.tick() => {
                    let now = Utc::now();
                    for (symbol, &source_id) in &active_sources {
                        if let Some(book) = orderbooks.get(&(source_id, symbol.clone())) {
                            emit_snapshot(
                                &mut writer,
                                symbol,
                                now,
                                book.exchange_timestamp,
                                book,
                            )?;
                        }
                    }
                }
            }
        }
    }
    .await;

    eprintln!("stopping market discovery and WebSocket shards");
    let _ = shutdown_tx.send(true);
    while events_rx.recv().await.is_some() {}
    eprintln!("market discovery and WebSocket shards stopped; flushing SBE output");
    let manager_result = manager.await;
    let flush_result = writer.flush();
    let manager_result = manager_result.context("market manager task failed")?;
    manager_result?;
    flush_result?;
    eprintln!("SBE output flushed; shutdown complete");
    outcome
}

fn snapshot_domain(
    symbol: &str,
    received_at: DateTime<Utc>,
    exchange_timestamp: DateTime<Utc>,
    book: &Orderbook,
) -> SnapshotDomain {
    SnapshotDomain {
        received_at,
        exchange_timestamp,
        bids: book
            .bids
            .iter()
            .rev()
            .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        asks: book
            .asks
            .iter()
            .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect(),
        symbol: symbol.to_owned(),
    }
}

fn emit_snapshot(
    writer: &mut AlignedSbeWriter,
    symbol: &str,
    received_at: DateTime<Utc>,
    exchange_timestamp: DateTime<Utc>,
    book: &Orderbook,
) -> anyhow::Result<()> {
    let snapshot = snapshot_domain(symbol, received_at, exchange_timestamp, book);
    writer.write_msg(&snapshot)?;
    println!(
        "{}",
        json!({
            "type": "snapshot",
            "received_at": received_at.to_rfc3339(),
            "exchange_timestamp": exchange_timestamp.to_rfc3339(),
            "symbol": symbol,
            "bids": snapshot.bids.iter().map(|level| [level.price_level, level.quantity]).collect::<Vec<_>>(),
            "asks": snapshot.asks.iter().map(|level| [level.price_level, level.quantity]).collect::<Vec<_>>(),
        })
    );
    Ok(())
}

fn should_promote_source(
    active_source: Option<usize>,
    active_clock: Option<u64>,
    incoming_source: usize,
    incoming_clock: u64,
) -> bool {
    active_source != Some(incoming_source)
        && (active_source.is_none()
            || active_clock.is_none()
            || active_clock.is_some_and(|clock| incoming_clock > clock))
}

fn apply_delta(book: &mut Orderbook, update: &mut PriceLevelUpdateDomain) -> anyhow::Result<()> {
    let levels = match update.side {
        Side::Buy => &mut book.bids,
        Side::Sell => &mut book.asks,
    };
    let previous = levels.get(&update.price_level).copied().unwrap_or(0);
    let quantity = previous
        .checked_add(update.quantity)
        .context("orderbook quantity overflow")?;
    anyhow::ensure!(
        quantity >= 0,
        "exchange delta would make {} {} level {} negative",
        update.symbol,
        update.side,
        update.price_level
    );
    if quantity == 0 {
        levels.remove(&update.price_level);
    } else {
        levels.insert(update.price_level, quantity);
    }
    book.exchange_timestamp = update.exchange_timestamp;
    update.quantity = quantity;
    Ok(())
}

async fn verify_websocket_auth(adapter: &dyn ExchangeAdapter) -> anyhow::Result<()> {
    let request = adapter
        .websocket_request()
        .with_context(|| format!("building {} WebSocket authentication check", adapter.name()))?;
    let (mut socket, _) = connect_async(request).await.map_err(|error| match error {
        WebSocketError::Http(response) => {
            let status = response.status();
            let request_id = response
                .headers()
                .get("x-request-id")
                .or_else(|| response.headers().get("x-amzn-trace-id"))
                .and_then(|value| value.to_str().ok())
                .map(|value| format!(" (request id: {value})"))
                .unwrap_or_default();
            let body = String::from_utf8_lossy(response.body().as_deref().unwrap_or_default())
                .chars()
                .take(300)
                .collect::<String>();
            let detail = if body.trim().is_empty() {
                String::new()
            } else {
                format!(" Response: {}", body.trim())
            };
            anyhow::anyhow!(
                "{} WebSocket authentication check failed with HTTP {status}{request_id}.{detail} Verify the API key ID, matching private key, and selected environment.",
                adapter.name()
            )
        }
        error => anyhow::anyhow!(
            "{} WebSocket authentication check failed: {error}",
            adapter.name()
        ),
    })?;
    socket
        .close(None)
        .await
        .with_context(|| format!("closing {} WebSocket authentication check", adapter.name()))?;
    eprintln!(
        "{} WebSocket authentication check succeeded",
        adapter.name()
    );
    Ok(())
}

async fn manage_markets(
    config: Config,
    adapter: Arc<dyn ExchangeAdapter>,
    client: reqwest::Client,
    events: mpsc::Sender<MarketEvent>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    eprintln!("discovering {} markets", adapter.name());
    let mut shards = Vec::<Shard>::new();
    let mut market_assignments = HashMap::new();
    let Some(mut current_markets) =
        fetch_markets_or_shutdown(adapter.as_ref(), &client, &mut shutdown).await?
    else {
        return Ok(());
    };
    update_shards(
        &config,
        Arc::clone(&adapter),
        &events,
        &mut shards,
        &mut market_assignments,
        &current_markets,
        shutdown.clone(),
    );
    eprintln!(
        "discovered {} markets from {}; assigned them across {} redundant WebSocket connection(s) (limit: {})",
        current_markets.len(),
        adapter.name(),
        shards.len(),
        config.max_connections
    );
    if current_markets.is_empty() {
        eprintln!(
            "no open markets were returned; the collector will keep checking every {} seconds",
            MARKET_REFRESH_INTERVAL.as_secs()
        );
    }

    let mut refresh = time::interval(MARKET_REFRESH_INTERVAL);
    refresh.tick().await;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = refresh.tick() => {
                let refreshed_markets = tokio::select! {
                    changed = shutdown.changed() => {
                        let _ = changed;
                        break;
                    }
                    result = adapter.discover_markets(&client) => result,
                };
                match refreshed_markets {
                    Ok(markets) => {
                        if markets != current_markets {
                            eprintln!(
                                "open market list changed: {} -> {} markets",
                                current_markets.len(),
                                markets.len()
                            );
                            update_shards(
                                &config,
                                Arc::clone(&adapter),
                                &events,
                                &mut shards,
                                &mut market_assignments,
                                &markets,
                                shutdown.clone(),
                            );
                            current_markets = markets;
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "failed to refresh {} markets; keeping current subscriptions: {error:#}",
                            adapter.name()
                        );
                    }
                }
            }
        }
    }

    for shard in shards {
        let _ = shard.assignments.send(HashSet::new());
        let _ = shard.task.await;
    }
    Ok(())
}

async fn fetch_markets_or_shutdown(
    adapter: &dyn ExchangeAdapter,
    client: &reqwest::Client,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<Option<HashSet<String>>> {
    tokio::select! {
        changed = shutdown.changed() => {
            let _ = changed;
            Ok(None)
        }
        result = adapter.discover_markets(client) => result.map(Some),
    }
}

fn update_shards(
    config: &Config,
    adapter: Arc<dyn ExchangeAdapter>,
    events: &mpsc::Sender<MarketEvent>,
    shards: &mut Vec<Shard>,
    market_assignments: &mut HashMap<String, usize>,
    markets: &HashSet<String>,
    shutdown: watch::Receiver<bool>,
) {
    let primary_assignments = assign_markets(
        markets,
        market_assignments,
        shards.len() / REDUNDANT_CONNECTIONS_PER_SHARD,
        effective_markets_per_shard(
            markets.len(),
            config.markets_per_connection,
            config.max_connections,
        ),
    );
    let assignments = redundant_assignments(&primary_assignments);

    while shards.len() < assignments.len() {
        let (assignments_tx, assignments_rx) = watch::channel(HashSet::new());
        let task = tokio::spawn(run_shard(
            shards.len(),
            config.clone(),
            Arc::clone(&adapter),
            assignments_rx,
            events.clone(),
            shutdown.clone(),
        ));
        shards.push(Shard {
            markets: HashSet::new(),
            assignments: assignments_tx,
            task,
        });
    }

    for (index, shard) in shards.iter_mut().enumerate() {
        let next = assignments[index].clone();
        if shard.markets != next {
            shard.markets = next.clone();
            let _ = shard.assignments.send(next);
        }
    }
}

fn effective_markets_per_shard(
    total_markets: usize,
    preferred_markets_per_shard: usize,
    max_connections: usize,
) -> usize {
    let max_primary_shards = max_connections / REDUNDANT_CONNECTIONS_PER_SHARD;
    preferred_markets_per_shard.max(total_markets.div_ceil(max_primary_shards))
}

fn redundant_assignments(primary: &[HashSet<String>]) -> Vec<HashSet<String>> {
    primary
        .iter()
        .flat_map(|markets| std::iter::repeat_n(markets.clone(), REDUNDANT_CONNECTIONS_PER_SHARD))
        .collect()
}

fn assign_markets(
    markets: &HashSet<String>,
    assignments: &mut HashMap<String, usize>,
    existing_shards: usize,
    markets_per_shard: usize,
) -> Vec<HashSet<String>> {
    assignments.retain(|ticker, _| markets.contains(ticker));
    let mut counts = vec![0; existing_shards];
    for &shard in assignments.values() {
        counts[shard] += 1;
    }

    let mut new_markets: Vec<_> = markets
        .iter()
        .filter(|ticker| !assignments.contains_key(*ticker))
        .cloned()
        .collect();
    new_markets.sort();
    for ticker in new_markets {
        let shard = counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count < markets_per_shard)
            .min_by_key(|(_, count)| **count)
            .map(|(index, _)| index)
            .unwrap_or_else(|| {
                counts.push(0);
                counts.len() - 1
            });
        counts[shard] += 1;
        assignments.insert(ticker, shard);
    }

    let mut by_shard = vec![HashSet::new(); counts.len()];
    for (ticker, &shard) in assignments.iter() {
        by_shard[shard].insert(ticker.clone());
    }
    by_shard
}

async fn run_shard(
    shard_id: usize,
    config: Config,
    adapter: Arc<dyn ExchangeAdapter>,
    mut assignments: watch::Receiver<HashSet<String>>,
    events: mpsc::Sender<MarketEvent>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut reconnect_delay = Duration::from_millis(250);
    let mut first_connection = true;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let markets = assignments.borrow_and_update().clone();
        if markets.is_empty() {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                changed = assignments.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
            }
            continue;
        }

        if first_connection {
            first_connection = false;
            let startup_delay = Duration::from_millis(shard_id as u64 * 200);
            tokio::select! {
                _ = time::sleep(startup_delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                changed = assignments.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    continue;
                }
            }
        }

        match connect_shard(
            shard_id,
            &config,
            adapter.as_ref(),
            &markets,
            &events,
            &mut assignments,
            &mut shutdown,
        )
        .await
        {
            ConnectOutcome::Shutdown => break,
            ConnectOutcome::Rotate => {
                let _ = events
                    .send(MarketEvent::Reset {
                        source_id: shard_id,
                        symbols: markets.iter().cloned().collect(),
                    })
                    .await;
                eprintln!(
                    "{} WebSocket shard {shard_id} rotating its connection",
                    adapter.name()
                );
                reconnect_delay = Duration::from_millis(250);
                time::sleep(reconnect_delay).await;
            }
            ConnectOutcome::Reassign(next) => {
                let _ = events
                    .send(MarketEvent::Reset {
                        source_id: shard_id,
                        symbols: markets.union(&next).cloned().collect(),
                    })
                    .await;
                reconnect_delay = Duration::from_millis(250);
                continue;
            }
            ConnectOutcome::Disconnected(reason) => {
                let _ = events
                    .send(MarketEvent::Reset {
                        source_id: shard_id,
                        symbols: markets.iter().cloned().collect(),
                    })
                    .await;
                eprintln!(
                    "{} WebSocket shard {shard_id} disconnected: {reason}; reconnecting in {:.2}s",
                    adapter.name(),
                    reconnect_delay.as_secs_f64()
                );
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    changed = assignments.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        let next = assignments.borrow().clone();
                        let _ = events
                            .send(MarketEvent::Reset {
                                source_id: shard_id,
                                symbols: markets.union(&next).cloned().collect(),
                            })
                            .await;
                    }
                    _ = time::sleep(reconnect_delay) => {}
                }
                reconnect_delay = (reconnect_delay * 2).min(RECONNECT_MAX_DELAY);
            }
        }
    }
}

enum ConnectOutcome {
    Shutdown,
    Reassign(HashSet<String>),
    Rotate,
    Disconnected(String),
}

async fn connect_shard(
    shard_id: usize,
    config: &Config,
    adapter: &dyn ExchangeAdapter,
    markets: &HashSet<String>,
    events: &mpsc::Sender<MarketEvent>,
    assignments: &mut watch::Receiver<HashSet<String>>,
    shutdown: &mut watch::Receiver<bool>,
) -> ConnectOutcome {
    let request = match adapter.websocket_request() {
        Ok(request) => request,
        Err(error) => {
            return ConnectOutcome::Disconnected(format!(
                "building {} WebSocket request: {error:#}",
                adapter.name()
            ));
        }
    };

    let (mut socket, _) = tokio::select! {
        changed = shutdown.changed() => {
            let _ = changed;
            return ConnectOutcome::Shutdown;
        }
        changed = assignments.changed() => {
            if changed.is_ok() {
                return ConnectOutcome::Reassign(assignments.borrow().clone());
            }
            return ConnectOutcome::Shutdown;
        }
        result = connect_async(request) => match result {
            Ok(connection) => connection,
            Err(error) => {
                return ConnectOutcome::Disconnected(format!("connecting: {error}"));
            }
        }
    };

    if let Err(error) = events
        .send(MarketEvent::Reset {
            source_id: shard_id,
            symbols: markets.iter().cloned().collect(),
        })
        .await
    {
        let _ = socket.close(None).await;
        return ConnectOutcome::Disconnected(format!("event receiver stopped: {error}"));
    }
    eprintln!(
        "{} WebSocket shard {shard_id} connected for {} markets",
        adapter.name(),
        markets.len()
    );
    let mut ordered_markets: Vec<_> = markets.iter().cloned().collect();
    ordered_markets.sort();
    if let Err(error) = socket
        .send(Message::Text(
            adapter.subscribe_message(1, &ordered_markets).into(),
        ))
        .await
    {
        return ConnectOutcome::Disconnected(format!("sending subscription: {error}"));
    }
    let mut last_sequence: Option<u64> = None;
    let mut subscription_id: Option<u64> = None;
    let mut command_id = 2_u64;
    let mut heartbeat = time::interval(SOCKET_HEARTBEAT_INTERVAL);
    heartbeat.tick().await;
    let mut last_ping: Option<Instant> = None;
    let mut health_check = time::interval(BOOK_HEALTH_CHECK_INTERVAL);
    health_check.tick().await;
    let mut activity_check = time::interval(Duration::from_secs(1));
    activity_check.tick().await;
    let mut activity: HashMap<_, _> = markets
        .iter()
        .map(|symbol| (symbol.clone(), MarketActivity::new(Instant::now())))
        .collect();
    let rotation_stagger = Duration::from_secs(
        (shard_id as u64 / REDUNDANT_CONNECTIONS_PER_SHARD as u64) * 5
            + (shard_id as u64 % REDUNDANT_CONNECTIONS_PER_SHARD as u64) * 30,
    );
    let rotate_at = Instant::now() + config.socket_rotation + rotation_stagger;

    loop {
        tokio::select! {
            _ = time::sleep_until(rotate_at) => {
                return ConnectOutcome::Rotate;
            }
            changed = shutdown.changed() => {
                let _ = socket.send(Message::Close(None)).await;
                let _ = changed;
                return ConnectOutcome::Shutdown;
            }
            changed = assignments.changed() => {
                let _ = socket.send(Message::Close(None)).await;
                if changed.is_err() {
                    return ConnectOutcome::Shutdown;
                }
                return ConnectOutcome::Reassign(assignments.borrow().clone());
            }
            _ = heartbeat.tick() => {
                if last_ping.is_some_and(|sent| sent.elapsed() >= SOCKET_PONG_TIMEOUT) {
                    return ConnectOutcome::Disconnected(format!(
                        "WebSocket did not answer a ping within {} seconds",
                        SOCKET_PONG_TIMEOUT.as_secs()
                    ));
                }
                if last_ping.is_none_or(|sent| sent.elapsed() >= SOCKET_PING_INTERVAL) {
                    if let Err(error) = socket.send(Message::Ping(Vec::new().into())).await {
                        return ConnectOutcome::Disconnected(format!("sending WebSocket ping: {error}"));
                    }
                    last_ping = Some(Instant::now());
                }
            }
            _ = health_check.tick() => {
                if let Some(sid) = subscription_id {
                    let request =
                        adapter.health_snapshot_message(command_id, sid, &ordered_markets);
                    command_id = command_id.wrapping_add(1).max(2);
                    if let Err(error) = socket.send(Message::Text(request.into())).await {
                        return ConnectOutcome::Disconnected(format!("requesting health snapshots: {error}"));
                    }
                }
            }
            _ = activity_check.tick() => {
                let now = Instant::now();
                let quiet: Vec<_> = activity
                    .iter_mut()
                    .filter_map(|(symbol, activity)| {
                        let below_threshold = activity.finish_window(
                            now,
                            config.activity_window,
                            config.min_updates_per_window,
                        );
                        if below_threshold && activity.ready_to_resubscribe(now) {
                            activity.mark_resubscribed(now, config.quiet_resubscribe_cooldown);
                            Some(symbol.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !quiet.is_empty() && let Some(sid) = subscription_id {
                    eprintln!(
                        "{} WebSocket shard {shard_id}: {} market(s) had fewer than {} orderbook updates in {} seconds; resubscribing: {}",
                        adapter.name(),
                        quiet.len(),
                        config.min_updates_per_window,
                        config.activity_window.as_secs(),
                        quiet.join(", ")
                    );
                    if events
                        .send(MarketEvent::Reset {
                            source_id: shard_id,
                            symbols: quiet.clone(),
                        })
                        .await
                        .is_err()
                    {
                        return ConnectOutcome::Disconnected("event receiver stopped".to_owned());
                    }
                    let requests = adapter.resubscribe_messages(command_id, sid, &quiet);
                    let request_count = requests.len() as u64;
                    for request in requests {
                        if let Err(error) = socket.send(Message::Text(request.into())).await {
                            return ConnectOutcome::Disconnected(format!("resubscribing quiet markets: {error}"));
                        }
                    }
                    command_id = command_id.wrapping_add(request_count).max(2);
                }
            }
            message = socket.next() => {
                let Some(message) = message else {
                    return ConnectOutcome::Disconnected("server closed the connection".to_owned());
                };
                let message = match message {
                    Ok(message) => message,
                    Err(error) => return ConnectOutcome::Disconnected(error.to_string()),
                };
                match message {
                    Message::Text(text) => {
                        let value: Value = match serde_json::from_str(&text) {
                            Ok(value) => value,
                            Err(error) => return ConnectOutcome::Disconnected(format!("invalid JSON message: {error}")),
                        };
                        if let Some(error) = adapter.error_message(&value) {
                            return ConnectOutcome::Disconnected(format!("server error: {error}"));
                        }
                        if let Some(sid) = adapter.subscription_id(&value) {
                            subscription_id = Some(sid);
                        }
                        if let Some(sequence) = adapter.sequence_number(&value) {
                            if let Some(previous) = last_sequence
                                && sequence != previous + 1
                            {
                                return ConnectOutcome::Disconnected(format!(
                                    "message sequence gap: expected {}, received {sequence}",
                                    previous + 1
                                ));
                            }
                            last_sequence = Some(sequence);
                        }
                        match adapter.decode_message(&value, shard_id) {
                            Ok(Some(event)) => {
                                if let MarketEvent::Delta { update, .. } = &event
                                    && let Some(activity) = activity.get_mut(&update.symbol)
                                {
                                    activity.record();
                                }
                                if events.send(event).await.is_err() {
                                    return ConnectOutcome::Disconnected("event receiver stopped".to_owned());
                                }
                            }
                            Ok(None) => {}
                            Err(error) => return ConnectOutcome::Disconnected(format!("invalid {} message: {error:#}", adapter.name())),
                        }
                    }
                    Message::Ping(payload) => {
                        if let Err(error) = socket.send(Message::Pong(payload)).await {
                            return ConnectOutcome::Disconnected(format!("sending pong: {error}"));
                        }
                    }
                    Message::Pong(_) => {
                        last_ping = None;
                    }
                    Message::Close(frame) => {
                        return ConnectOutcome::Disconnected(format!("server closed the connection: {frame:?}"));
                    }
                    Message::Binary(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn applying_delta_writes_absolute_quantity_and_removes_empty_levels() {
        let timestamp = Utc::now();
        let mut book = Orderbook {
            bids: BTreeMap::from([(500_000, 100_000_000)]),
            asks: BTreeMap::new(),
            exchange_timestamp: timestamp,
            clock: 10,
        };
        let mut update = PriceLevelUpdateDomain {
            backtest_timestamp: timestamp,
            exchange_timestamp: timestamp,
            price_level: 500_000,
            quantity: -100_000_000,
            side: Side::Buy,
            symbol: "TEST-MARKET".to_owned(),
        };

        apply_delta(&mut book, &mut update).unwrap();

        assert_eq!(update.quantity, 0);
        assert!(!book.bids.contains_key(&500_000));
    }

    #[test]
    fn market_changes_keep_existing_symbols_on_their_websocket_shards() {
        let initial = HashSet::from([
            "MARKET-A".to_owned(),
            "MARKET-B".to_owned(),
            "MARKET-C".to_owned(),
            "MARKET-D".to_owned(),
        ]);
        let mut assignments = HashMap::new();
        let first = assign_markets(&initial, &mut assignments, 0, 2);
        assert_eq!(first.len(), 2);
        let retained_assignments = assignments.clone();

        let changed = HashSet::from([
            "MARKET-A".to_owned(),
            "MARKET-C".to_owned(),
            "MARKET-D".to_owned(),
            "MARKET-E".to_owned(),
        ]);
        let second = assign_markets(&changed, &mut assignments, first.len(), 2);

        assert_eq!(second.len(), first.len());
        for ticker in ["MARKET-A", "MARKET-C", "MARKET-D"] {
            assert_eq!(assignments[ticker], retained_assignments[ticker]);
        }
        assert_eq!(assignments.len(), changed.len());
        assert!(second.iter().all(|shard| shard.len() <= 2));
    }

    #[test]
    fn redundant_socket_assignments_cover_every_market_twice() {
        let markets = HashSet::from([
            "MARKET-A".to_owned(),
            "MARKET-B".to_owned(),
            "MARKET-C".to_owned(),
        ]);
        let mut ownership = HashMap::new();
        let primary = assign_markets(&markets, &mut ownership, 0, 2);
        let redundant = redundant_assignments(&primary);

        assert_eq!(
            redundant.len(),
            primary.len() * REDUNDANT_CONNECTIONS_PER_SHARD
        );
        for index in (0..redundant.len()).step_by(REDUNDANT_CONNECTIONS_PER_SHARD) {
            assert_eq!(
                redundant[index],
                redundant[index + REDUNDANT_CONNECTIONS_PER_SHARD - 1]
            );
        }

        for market in &markets {
            assert_eq!(
                redundant
                    .iter()
                    .filter(|assigned| assigned.contains(market))
                    .count(),
                REDUNDANT_CONNECTIONS_PER_SHARD
            );
        }
    }

    #[test]
    fn adaptive_shard_size_respects_connection_limit_and_preference() {
        assert_eq!(effective_markets_per_shard(250_001, 500, 200), 2_501);
        assert_eq!(effective_markets_per_shard(20_000, 500, 200), 500);
        assert_eq!(effective_markets_per_shard(20_000, 1_000, 200), 1_000);
    }

    #[test]
    fn redundant_source_promotes_only_when_it_is_more_current() {
        assert!(should_promote_source(Some(0), Some(100), 1, 101));
        assert!(!should_promote_source(Some(0), Some(100), 1, 100));
        assert!(!should_promote_source(Some(0), Some(100), 1, 99));
        assert!(should_promote_source(Some(0), None, 1, 1));
        assert!(!should_promote_source(Some(1), Some(100), 1, 101));
    }

    #[test]
    fn activity_windows_flag_low_counts_and_reset_after_each_window() {
        let started = Instant::now();
        let mut activity = MarketActivity::new(started);
        activity.record();

        assert!(!activity.finish_window(
            started + Duration::from_secs(59),
            Duration::from_secs(60),
            2
        ));
        assert!(activity.finish_window(
            started + Duration::from_secs(60),
            Duration::from_secs(60),
            2
        ));
        activity.record();
        assert!(!activity.finish_window(
            started + Duration::from_secs(120),
            Duration::from_secs(60),
            1
        ));
    }

    #[test]
    fn activity_windows_accept_threshold_and_detect_no_updates() {
        let started = Instant::now();
        let mut active = MarketActivity::new(started);
        active.record();
        active.record();
        assert!(!active.finish_window(
            started + Duration::from_secs(60),
            Duration::from_secs(60),
            2
        ));

        let quiet = MarketActivity::new(started);
        let mut quiet = quiet;
        assert!(quiet.finish_window(
            started + Duration::from_secs(60),
            Duration::from_secs(60),
            1
        ));
    }

    #[test]
    fn quiet_market_resubscription_has_a_cooldown() {
        let started = Instant::now();
        let mut activity = MarketActivity::new(started);
        assert!(activity.finish_window(
            started + Duration::from_secs(60),
            Duration::from_secs(60),
            1
        ));
        activity.mark_resubscribed(started + Duration::from_secs(60), Duration::from_secs(300));
        assert!(!activity.ready_to_resubscribe(started + Duration::from_secs(359)));
        assert!(activity.ready_to_resubscribe(started + Duration::from_secs(360)));
    }
}
