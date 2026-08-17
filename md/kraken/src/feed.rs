//! One websocket connection: subscribe to `level3`, normalize every order
//! event onto the nlib wire, and hand the frames to the publisher.
//!
//! Normalization: bids are buys and asks sells; a snapshot replays as a
//! `Clear` followed by an `Add` per resting order; update events map add ->
//! `Add`, modify -> `Modify` (new remaining quantity, price unchanged, queue
//! priority kept), delete -> `Cancel` (the order leaves the book). Prices and
//! quantities are parsed from the raw JSON text into fixed point, ids and
//! symbols hash to the wire's integer ids, and `seq` is assigned from one
//! process-wide counter, so the ZMQ stream is a single ordered feed. An order
//! whose fields fail to normalize is skipped and counted, never sent
//! half-formed.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::{self, Credentials};
use crate::stats::Stats;
use crate::wire::{self, Frame};

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

/// A session that lasted this long counts as healthy, so the next failure
/// starts backing off from the beginning rather than from wherever the last
/// outage left off.
const HEALTHY_SESSION: Duration = Duration::from_secs(120);

/// A subscription's rate-limit cost, per symbol, by book depth. The budget is
/// 200 per second on the standard tier (500 on pro) and is account-wide, so
/// spending it is paced globally by [`SubscribePacer`].
fn subscribe_cost(depth: u32) -> u32 {
    match depth {
        10 => 5,
        100 => 25,
        _ => 100,
    }
}

/// State shared by every connection.
pub struct Shared {
    pub creds: Credentials,
    pub http: reqwest::Client,
    pub stats: Stats,
    pub pacer: SubscribePacer,
    /// Feed sequence numbers, one stream across all connections.
    pub seq: AtomicI64,
    /// Framed records bound for the publisher.
    pub frames: mpsc::Sender<Frame>,
}

/// Grants one subscribe batch per second across every connection. Batches
/// are sized to cost at most half the standard tier's per-second budget, and
/// the budget is shared by the whole account — per-connection pacing would
/// multiply the spend by the connection count.
pub struct SubscribePacer(Mutex<Instant>);

impl SubscribePacer {
    pub fn new() -> Self {
        Self(Mutex::new(Instant::now()))
    }

    /// Waits for this caller's slot; slots are one second apart globally.
    async fn wait(&self) {
        let at = {
            let mut next = self.0.lock().await;
            let at = (*next).max(Instant::now());
            *next = at + Duration::from_secs(1);
            at
        };
        tokio::time::sleep_until(at).await;
    }
}

/// Runs a connection until the process ends, reconnecting on any failure with
/// capped exponential backoff. Credentials are validated before any of these
/// are spawned, so every failure here is worth retrying.
pub async fn run(id: usize, symbols: Arc<Vec<String>>, depth: u32, shared: Arc<Shared>) {
    let mut delay = RECONNECT_DELAY_MIN;
    loop {
        let started = Instant::now();

        // A token is good for 15 minutes before use, so take a fresh one per
        // attempt rather than holding one across a long outage.
        match auth::websockets_token(&shared.http, &shared.creds).await {
            Ok(token) => match session(id, &symbols, depth, &token, &shared).await {
                Ok(()) => eprintln!("[conn {id}] closed by peer, reconnecting"),
                Err(error) => eprintln!("[conn {id}] {error:#}, reconnecting"),
            },
            Err(error) => eprintln!("[conn {id}] {error:#}, retrying"),
        }
        shared.stats.note_disconnect();

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
    shared: &Arc<Shared>,
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
    let pace = Arc::clone(shared);
    let subscriber = tokio::spawn(async move {
        for request in requests {
            pace.pacer.wait().await;
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
            Some(Ok(Message::Text(text))) => handle(id, text.as_str(), shared).await?,
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

/// Splits the symbols into subscribe requests small enough that one request
/// per second stays inside the global pacer's budget.
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

/// The fields of an inbound message this client reads. Prices and quantities
/// are kept as raw JSON text so fixed-point conversion is exact.
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
    data: Vec<Book<'a>>,
}

#[derive(Deserialize)]
struct Book<'a> {
    #[serde(borrow, default)]
    symbol: Option<&'a str>,
    #[serde(borrow, default)]
    bids: Vec<OrderMsg<'a>>,
    #[serde(borrow, default)]
    asks: Vec<OrderMsg<'a>>,
}

#[derive(Deserialize)]
struct OrderMsg<'a> {
    #[serde(default)]
    event: Option<EventKind>,
    #[serde(borrow)]
    order_id: &'a str,
    #[serde(borrow)]
    limit_price: &'a RawValue,
    #[serde(borrow)]
    order_qty: &'a RawValue,
    #[serde(borrow)]
    timestamp: &'a str,
}

/// What an order in a `level3` update did. Absent in a snapshot, where every
/// order is simply present.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum EventKind {
    Add,
    Modify,
    Delete,
}

async fn handle(id: usize, text: &str, shared: &Arc<Shared>) -> Result<()> {
    let stats = &shared.stats;
    stats.note_bytes(text.len() as u64);

    let message: Inbound = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            eprintln!("[conn {id}] unparseable message: {error}");
            return Ok(());
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
        return Ok(());
    }
    if message.channel != Some("level3") {
        return Ok(());
    }

    let snapshot = match message.kind {
        Some("snapshot") => true,
        Some("update") => false,
        _ => return Ok(()),
    };
    for book in &message.data {
        let Some(symbol) = book.symbol else {
            stats.note_norm_error();
            continue;
        };
        let instrument_id = wire::instrument_id(symbol);

        // Normalize the whole message before sending any of it: a snapshot's
        // Clear carries the newest event time its orders replay up to.
        let mut orders = Vec::with_capacity(book.bids.len() + book.asks.len() + 1);
        let (mut adds, mut modifies, mut deletes) = (0, 0, 0);
        for (side, msgs) in [(wire::Side::Buy, &book.bids), (wire::Side::Sell, &book.asks)] {
            for msg in msgs {
                let action = match (snapshot, msg.event) {
                    (true, _) => wire::Action::Add,
                    (false, Some(EventKind::Add)) => wire::Action::Add,
                    (false, Some(EventKind::Modify)) => wire::Action::Modify,
                    (false, Some(EventKind::Delete)) => wire::Action::Cancel,
                    (false, None) => {
                        stats.note_norm_error();
                        continue;
                    }
                };
                let normalized = (
                    wire::scaled(msg.limit_price.get(), wire::PRICE_DECIMALS),
                    wire::scaled(msg.order_qty.get(), wire::QTY_DECIMALS),
                    wire::event_ns(msg.timestamp),
                );
                let (Some(price), Some(qty), Some(event_ns)) = normalized else {
                    stats.note_norm_error();
                    continue;
                };
                match action {
                    wire::Action::Add => adds += 1,
                    wire::Action::Modify => modifies += 1,
                    _ => deletes += 1,
                }
                orders.push(wire::Order {
                    seq: 0, // assigned at send, after the Clear takes its slot
                    order_id: wire::order_id(msg.order_id),
                    price,
                    qty,
                    event_ns,
                    instrument_id,
                    side,
                    action,
                });
            }
        }

        if snapshot {
            let event_ns = orders.iter().map(|o| o.event_ns).max().unwrap_or(0);
            forward(
                shared,
                wire::Order {
                    seq: 0,
                    order_id: 0,
                    price: 0,
                    qty: 0,
                    event_ns,
                    instrument_id,
                    side: wire::Side::Buy,
                    action: wire::Action::Clear,
                },
            )
            .await?;
            stats.note_snapshot(orders.len() as u64);
        } else {
            stats.note_update(adds, modifies, deletes);
        }
        for order in orders {
            forward(shared, order).await?;
        }
    }
    Ok(())
}

/// Stamps `order` with the next sequence number and queues its frame for the
/// publisher, waiting while the channel is full; a record is delayed, never
/// dropped, before the PUB socket.
async fn forward(shared: &Arc<Shared>, mut order: wire::Order) -> Result<()> {
    order.seq = shared.seq.fetch_add(1, Ordering::Relaxed) + 1;
    shared
        .frames
        .send(order.encode())
        .await
        .map_err(|_| anyhow::anyhow!("publisher stopped"))
}

/// Aborts a task when it goes out of scope.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
