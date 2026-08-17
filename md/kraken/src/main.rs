//! Reads Kraken's spot `level3` (L3 orders) websocket feed for the most
//! actively traded crypto pairs, normalizes each order event onto the nlib
//! wire, and publishes the framed records on a ZMQ PUB socket for nqbook's
//! feed thread.

mod auth;
mod feed;
mod publish;
mod stats;
mod symbols;
mod wire;

use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

use crate::auth::Credentials;
use crate::feed::Shared;
use crate::stats::Stats;

#[derive(Parser)]
#[command(about, version)]
struct Args {
    /// How many of the most active pairs to subscribe to, over one
    /// connection. The subscribe request costs 5 rate-limit points per
    /// symbol at depth 10 against a budget of 200 per second (standard
    /// tier; 500 on pro), so the tier caps how high this can go; Kraken
    /// caps a connection at 200 symbols regardless.
    #[arg(long, default_value_t = 35, value_parser = clap::value_parser!(u16).range(1..=200))]
    symbols: u16,

    /// Book depth per symbol; Kraken accepts 10, 100 or 1000.
    #[arg(long, default_value_t = 10, value_parser = parse_depth)]
    depth: u32,

    /// ZMQ PUB bind endpoint; nqbook's SUB connects here.
    #[arg(long, default_value = "tcp://0.0.0.0:5555")]
    endpoint: String,

    /// Seconds between throughput reports.
    #[arg(long, default_value_t = 10)]
    report_interval: u64,

    /// Print `instrument_id,symbol` for the selected pairs and exit without
    /// connecting.
    #[arg(long)]
    list_symbols: bool,
}

fn parse_depth(value: &str) -> Result<u32, String> {
    match value.parse() {
        Ok(depth @ (10 | 100 | 1000)) => Ok(depth),
        _ => Err("depth must be 10, 100 or 1000".to_owned()),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build http client")?;

    let selected = symbols::select(&http, args.symbols as usize).await?;
    eprintln!("selected the {} most active crypto pairs", selected.len());
    if args.list_symbols {
        for symbol in &selected {
            println!("{},{symbol}", wire::instrument_id(symbol));
        }
        return Ok(());
    }

    // Prove the key works before connecting, so a bad credential fails here
    // with Kraken's own message instead of becoming a retry loop.
    let creds = Credentials::load()?;
    auth::websockets_token(&http, &creds)
        .await
        .context("credentials rejected")?;

    let (frames_tx, frames_rx) = tokio::sync::mpsc::channel(1 << 16);
    let shared = Arc::new(Shared {
        creds,
        http,
        stats: Stats::default(),
        seq: AtomicI64::new(0),
        frames: frames_tx,
    });

    let publisher = tokio::spawn(publish::run(args.endpoint, frames_rx, Arc::clone(&shared)));
    let connection = tokio::spawn(feed::run(
        Arc::new(selected),
        args.depth,
        Arc::clone(&shared),
    ));

    let reporter = tokio::spawn(report(Arc::clone(&shared), args.report_interval));

    // The connection retries forever, so a publisher failure, a panic, or
    // Ctrl-C ends the run.
    let result = tokio::select! {
        outcome = publisher => outcome.context("publisher panicked")?,
        outcome = connection => outcome.context("connection task panicked"),
        _ = tokio::signal::ctrl_c() => Ok(()),
    };

    reporter.abort();
    let total = shared.stats.snapshot();
    eprintln!(
        "\ntotals: {} snapshots, {} updates, {} orders, {} published, {:.1} MB received",
        total.snapshots,
        total.updates,
        total.orders,
        total.published,
        total.bytes as f64 / (1 << 20) as f64,
    );
    result
}

async fn report(shared: Arc<Shared>, interval_secs: u64) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    ticker.tick().await;

    let mut previous = shared.stats.snapshot();
    let mut last = tokio::time::Instant::now();
    loop {
        ticker.tick().await;
        let now = tokio::time::Instant::now();
        let current = shared.stats.snapshot();
        eprintln!("{}", current.report(&previous, now - last));
        previous = current;
        last = now;
    }
}
