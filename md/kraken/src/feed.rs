//! One websocket connection: subscribe to `level3` and drain it.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::{self, Credentials};
use crate::stats::{EventKind, Stats};

/// `level3` lives on its own endpoint, not the general authenticated one.
const WS_URL: &str = "wss://ws-l3.kraken.com/v2";

/// Kraken caps a connection at 200 symbols; more requires more connections.
pub const MAX_SYMBOLS_PER_CONNECTION: usize = 200;

/// Kraken sends a heartbeat about once a second when no channel is updating,
/// so silence this long means the connection is dead however healthy the
/// socket looks.
const STALL_TIMEOUT: Duration = Duration::from_secs(20);

const RECONNECT_DELAY_MIN: Duration = Duration::from_secs(1);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(60);

/// A subscription's rate-limit cost, per symbol, by book depth. The standard
/// tier allows 200 per second, the pro tier 500.
fn subscribe_cost(depth: u32) -> u32 {
    match depth {
        10 => 5,
        100 => 25,
        _ => 100,
    }
}

/// A session that lasted this long counts as healthy, so the next failure
/// starts backing off from the beginning rather than from wherever the last
/// outage left off.
const HEALTHY_SESSION: Duration = Duration::from_secs(120);

/// Runs a connection until the process ends, reconnecting on any failure with
/// capped exponential backoff. Credentials are validated before any of these
/// are spawned, so every failure here is worth retrying.
pub async fn run(
    id: usize,
    symbols: Arc<Vec<String>>,
    depth: u32,
    creds: Arc<Credentials>,
    http: reqwest::Client,
    stats: Arc<Stats>,
) {
    let mut delay = RECONNECT_DELAY_MIN;
    loop {
        let started = Instant::now();

        // A token is good for 15 minutes before use, so take a fresh one per
        // attempt rather than holding one across a long outage.
        match auth::websockets_token(&http, &creds).await {
            Ok(token) => match session(id, &symbols, depth, &token, &stats).await {
                Ok(()) => eprintln!("[conn {id}] closed by peer, reconnecting"),
                Err(error) => eprintln!("[conn {id}] {error:#}, reconnecting"),
            },
            Err(error) => eprintln!("[conn {id}] {error:#}, retrying"),
        }
        stats.note_disconnect();

        if started.elapsed() >= HEALTHY_SESSION {
            delay = RECONNECT_DELAY_MIN;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(RECONNECT_DELAY_MAX);
    }
}

async fn session(
    id: usize,
    symbols: &[String],
    depth: u32,
    token: &str,
    stats: &Stats,
) -> Result<()> {
    let (socket, _) = tokio_tungstenite::connect_async(WS_URL)
        .await
        .with_context(|| format!("connect to {WS_URL}"))?;
    eprintln!("[conn {id}] connected, subscribing to {} symbols", symbols.len());

    let (mut sink, mut stream) = socket.split();

    // Subscribe from a separate task so the reader keeps draining while the
    // batches are paced out; snapshots start arriving before the last batch
    // is sent and would otherwise fill the socket buffer.
    let requests = subscribe_requests(symbols, depth, token);
    let subscriber = tokio::spawn(async move {
        for (batch, request) in requests.into_iter().enumerate() {
            if batch > 0 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            if sink.send(Message::Text(request.into())).await.is_err() {
                return;
            }
        }
    });
    // Cancelling on the way out closes the sink, which closes the connection.
    let _guard = AbortOnDrop(subscriber);

    loop {
        let message = timeout(STALL_TIMEOUT, stream.next())
            .await
            .map_err(|_| anyhow::anyhow!("no message for {STALL_TIMEOUT:?}"))?;

        match message {
            Some(Ok(Message::Text(text))) => handle(id, text.as_str(), stats),
            Some(Ok(Message::Close(frame))) => {
                bail!("server closed the connection: {frame:?}");
            }
            // Ping/Pong are answered by the library; Binary is never sent.
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error).context("websocket read failed"),
            None => return Ok(()),
        }
    }
}

/// Splits the symbols into subscribe requests small enough to stay inside the
/// per-second rate budget when sent one batch a second.
fn subscribe_requests(symbols: &[String], depth: u32, token: &str) -> Vec<String> {
    // Half the standard tier's 200/second, to leave room for a shared key.
    const BUDGET_PER_SECOND: u32 = 100;
    let batch = (BUDGET_PER_SECOND / subscribe_cost(depth)).max(1) as usize;

    symbols
        .chunks(batch)
        .enumerate()
        .map(|(index, chunk)| {
            let request = SubscribeRequest {
                method: "subscribe",
                req_id: index as u64 + 1,
                params: SubscribeParams {
                    channel: "level3",
                    symbol: chunk,
                    depth,
                    snapshot: true,
                    token,
                },
            };
            serde_json::to_string(&request).expect("subscribe request is serializable")
        })
        .collect()
}

#[derive(serde::Serialize)]
struct SubscribeRequest<'a> {
    method: &'a str,
    req_id: u64,
    params: SubscribeParams<'a>,
}

#[derive(serde::Serialize)]
struct SubscribeParams<'a> {
    channel: &'a str,
    symbol: &'a [String],
    depth: u32,
    snapshot: bool,
    token: &'a str,
}

/// The fields of an inbound message this client cares about. Everything else,
/// including every price and quantity, is parsed past and dropped.
#[derive(Deserialize)]
struct Inbound<'a> {
    #[serde(borrow, default)]
    channel: Option<&'a str>,
    #[serde(borrow, default, rename = "type")]
    kind: Option<&'a str>,
    #[serde(borrow, default)]
    method: Option<&'a str>,
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    data: Vec<Book>,
}

#[derive(Deserialize)]
struct Book {
    #[serde(default)]
    bids: Vec<Order>,
    #[serde(default)]
    asks: Vec<Order>,
}

#[derive(Deserialize)]
struct Order {
    #[serde(default)]
    event: Option<EventKind>,
}

fn handle(id: usize, text: &str, stats: &Stats) {
    stats.note_bytes(text.len() as u64);

    let message: Inbound = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            eprintln!("[conn {id}] unparseable message: {error}");
            return;
        }
    };

    if message.method == Some("subscribe") {
        if message.success == Some(false) {
            eprintln!(
                "[conn {id}] subscribe rejected: {}",
                message.error.as_deref().unwrap_or("unknown error")
            );
            stats.note_subscribe_failure();
        }
        return;
    }
    if message.channel != Some("level3") {
        return;
    }

    let orders = message
        .data
        .iter()
        .flat_map(|book| book.bids.iter().chain(&book.asks));
    match message.kind {
        Some("snapshot") => stats.note_snapshot(orders.count() as u64),
        Some("update") => stats.note_update(orders.map(|order| order.event)),
        _ => {}
    }
}

/// Aborts a task when it goes out of scope.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
