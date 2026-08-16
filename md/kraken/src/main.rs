//! Subscribes to Kraken's spot `level3` (L3 orders) websocket feed for the
//! most actively traded USD pairs and discards everything it receives. It
//! exists to exercise the feed and measure its shape and rate; nothing is
//! persisted.

mod auth;
mod feed;
mod stats;
mod symbols;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

use crate::auth::Credentials;
use crate::feed::MAX_SYMBOLS_PER_CONNECTION;
use crate::stats::Stats;

#[derive(Parser)]
#[command(about, version)]
struct Args {
    /// How many of the most active crypto/USD pairs to subscribe to. Kraken
    /// allows 200 symbols per connection, so more opens more connections.
    #[arg(long, default_value_t = 200)]
    symbols: usize,

    /// Book depth per symbol; Kraken accepts 10, 100 or 1000.
    #[arg(long, default_value_t = 10, value_parser = parse_depth)]
    depth: u32,

    /// Seconds between throughput reports.
    #[arg(long, default_value_t = 10)]
    report_interval: u64,

    /// Print the selected symbols and exit without connecting.
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

    let selected = symbols::most_active(&http, args.symbols).await?;
    eprintln!(
        "selected {} of the {} most active crypto/USD pairs",
        selected.len(),
        args.symbols
    );
    if args.list_symbols {
        for symbol in &selected {
            println!("{symbol}");
        }
        return Ok(());
    }

    // Prove the key works before spawning connections, so a bad credential
    // fails here with Kraken's own message instead of becoming a retry loop.
    let creds = Arc::new(Credentials::load()?);
    auth::websockets_token(&http, &creds)
        .await
        .context("credentials rejected")?;

    let stats = Arc::new(Stats::default());

    let mut connections = tokio::task::JoinSet::new();
    for (id, shard) in selected.chunks(MAX_SYMBOLS_PER_CONNECTION).enumerate() {
        connections.spawn(feed::run(
            id,
            Arc::new(shard.to_vec()),
            args.depth,
            Arc::clone(&creds),
            http.clone(),
            Arc::clone(&stats),
        ));
    }

    let reporter = tokio::spawn(report(Arc::clone(&stats), args.report_interval));

    // Connections retry forever, so only a panic or Ctrl-C ends the run.
    let result = tokio::select! {
        Some(outcome) = connections.join_next() => outcome.context("connection task panicked"),
        _ = tokio::signal::ctrl_c() => Ok(()),
    };

    reporter.abort();
    let total = stats.snapshot();
    eprintln!(
        "\ntotals: {} snapshots, {} updates, {} orders, {:.1} MB received",
        total.snapshots,
        total.updates,
        total.orders,
        total.bytes as f64 / (1 << 20) as f64,
    );
    result
}

async fn report(stats: Arc<Stats>, interval_secs: u64) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    ticker.tick().await;

    let mut previous = stats.snapshot();
    let mut last = tokio::time::Instant::now();
    loop {
        ticker.tick().await;
        let now = tokio::time::Instant::now();
        let current = stats.snapshot();
        eprintln!("{}", current.report(&previous, now - last));
        previous = current;
        last = now;
    }
}
