//! The websocket connections. One connection per channel, each subscribing
//! once and then normalizing every inbound message onto nlib records for the
//! publisher.
//!
//! A connection reconnects on any failure with capped exponential backoff,
//! taking a fresh websocket token per attempt where the channel needs one.
//! Both connections draw sequence numbers from one counter and feed one PUB
//! socket, so nqbook reads a single ordered stream.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::Message;

use crate::auth::{self, Credentials};
use crate::stats::Stats;
use crate::wire::Frame;

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

/// State shared between the connections and the publisher.
pub struct Shared {
    pub creds: Credentials,
    pub http: reqwest::Client,
    pub stats: Stats,
    /// Feed sequence numbers.
    pub seq: AtomicI64,
    /// Framed records bound for the publisher.
    pub frames: mpsc::Sender<Frame>,
}

impl Shared {
    /// Takes the next feed sequence number.
    pub fn next_seq(&self) -> i64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// One Kraken channel: where it is served, how it is subscribed to, and how
/// its messages become frames. Connecting, reconnecting, stall detection and
/// publishing are the same for every channel and live in this module.
pub struct Channel {
    /// Websocket endpoint serving the channel.
    pub url: &'static str,
    /// Whether the subscribe request carries a websocket token.
    pub authenticated: bool,
    /// Builds the subscribe request. `token` is empty for a public channel.
    pub subscribe: fn(symbols: &[String], depth: u32, token: &str) -> String,
    /// Normalizes one inbound message, appending a frame per record.
    pub normalize: fn(text: &str, shared: &Shared, out: &mut Vec<Frame>),
}

/// Runs `channel`'s connection until the process ends, reconnecting on any
/// failure. Credentials are validated before this is spawned, so every
/// failure here is worth retrying.
pub async fn run(channel: Channel, symbols: Arc<Vec<String>>, depth: u32, shared: Arc<Shared>) {
    let mut delay = RECONNECT_DELAY_MIN;
    loop {
        let started = Instant::now();
        match token(&channel, &shared).await {
            Ok(token) => match session(&channel, &symbols, depth, &token, &shared).await {
                Ok(()) => eprintln!("{} closed by peer, reconnecting", channel.url),
                Err(error) => eprintln!("{error:#}, reconnecting"),
            },
            Err(error) => eprintln!("{error:#}, retrying"),
        }
        shared.stats.note_disconnect();

        if started.elapsed() >= HEALTHY_SESSION {
            delay = RECONNECT_DELAY_MIN;
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(RECONNECT_DELAY_MAX);
    }
}

/// A token is good for 15 minutes before use, so an authenticated channel
/// takes a fresh one per attempt rather than holding one across a long
/// outage. A public channel subscribes with an empty token.
async fn token(channel: &Channel, shared: &Shared) -> Result<String> {
    if channel.authenticated {
        auth::websockets_token(&shared.http, &shared.creds).await
    } else {
        Ok(String::new())
    }
}

async fn session(
    channel: &Channel,
    symbols: &[String],
    depth: u32,
    token: &str,
    shared: &Shared,
) -> Result<()> {
    let url = channel.url;
    let (mut socket, _) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| format!("connect to {url}"))?;
    eprintln!("connected to {url}, subscribing to {} symbols", symbols.len());

    // One request subscribes every symbol; its rate-limit cost is what caps
    // --symbols (see main.rs).
    socket
        .send(Message::Text((channel.subscribe)(symbols, depth, token).into()))
        .await
        .with_context(|| format!("send subscribe request to {url}"))?;

    let mut frames = Vec::new();
    loop {
        let message = timeout(STALL_TIMEOUT, socket.next())
            .await
            .map_err(|_| anyhow::anyhow!("{url} sent no message for {STALL_TIMEOUT:?}"))?;

        match message {
            Some(Ok(Message::Text(text))) => {
                shared.stats.note_bytes(text.len() as u64);
                (channel.normalize)(text.as_str(), shared, &mut frames);
                for frame in frames.drain(..) {
                    // A record is delayed rather than dropped before the PUB
                    // socket, so the channel's capacity absorbs a burst.
                    shared
                        .frames
                        .send(frame)
                        .await
                        .map_err(|_| anyhow::anyhow!("publisher stopped"))?;
                }
            }
            Some(Ok(Message::Close(frame))) => bail!("{url} closed the connection: {frame:?}"),
            // Ping/Pong are answered by the library; Binary is never sent.
            Some(Ok(_)) => {}
            Some(Err(error)) => return Err(error).with_context(|| format!("{url} read failed")),
            None => return Ok(()),
        }
    }
}

/// Reports a rejected subscribe request. Returns whether the message is a
/// subscribe reply, which carries no channel data.
pub fn subscribe_reply(
    method: Option<&str>,
    success: Option<bool>,
    error: Option<&str>,
    stats: &Stats,
) -> bool {
    if method != Some("subscribe") {
        return false;
    }
    if success == Some(false) {
        eprintln!("subscribe rejected: {}", error.unwrap_or("unknown error"));
        stats.note_subscribe_failure();
    }
    true
}
