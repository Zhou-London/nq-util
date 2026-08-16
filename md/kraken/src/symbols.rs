//! Selects which pairs to subscribe to, ranked by traded volume.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const REST_BASE: &str = "https://api.kraken.com";

/// Kraken's classic asset codes prefix fiat with `Z` (`ZUSD`, `ZEUR`). Pairs
/// whose base is fiat are FX crosses, not crypto, so they are excluded.
const FIAT_PREFIX: char = 'Z';

/// The quote asset to rank against; USD books carry most of Kraken's volume.
const QUOTE: &str = "ZUSD";

#[derive(Deserialize)]
struct Envelope<T> {
    #[serde(default)]
    error: Vec<String>,
    result: Option<T>,
}

#[derive(Deserialize)]
struct AssetPair {
    /// The `BASE/QUOTE` name the websocket API uses; absent for a few pairs
    /// that are not offered over websocket.
    wsname: Option<String>,
    base: String,
    quote: String,
    status: String,
}

#[derive(Deserialize)]
struct Ticker {
    /// `[today, last 24 hours]` traded volume, in base units.
    v: [String; 2],
    /// `[today, last 24 hours]` volume-weighted average price.
    p: [String; 2],
}

/// Returns the `limit` most active online crypto/USD pairs as websocket names,
/// most traded first, ranked by 24-hour volume valued at that day's VWAP.
pub async fn most_active(http: &reqwest::Client, limit: usize) -> Result<Vec<String>> {
    let pairs: HashMap<String, AssetPair> = get(http, "/0/public/AssetPairs").await?;
    let tickers: HashMap<String, Ticker> = get(http, "/0/public/Ticker").await?;

    let mut ranked: Vec<(f64, String)> = pairs
        .iter()
        .filter(|(_, pair)| {
            pair.status == "online"
                && pair.quote == QUOTE
                && !pair.base.starts_with(FIAT_PREFIX)
                && pair.wsname.is_some()
        })
        .filter_map(|(id, pair)| {
            let ticker = tickers.get(id)?;
            let volume: f64 = ticker.v[1].parse().ok()?;
            let vwap: f64 = ticker.p[1].parse().ok()?;
            let notional = volume * vwap;
            let name = websocket_v2_name(pair.wsname.as_deref().expect("filtered above"));
            (notional > 0.0).then_some((notional, name))
        })
        .collect();

    if ranked.is_empty() {
        bail!("no tradeable {QUOTE} pairs found");
    }
    ranked.sort_unstable_by(|a, b| b.0.total_cmp(&a.0));
    ranked.truncate(limit);
    Ok(ranked.into_iter().map(|(_, name)| name).collect())
}

/// Websocket v2 renamed two assets that `wsname` still reports under their
/// classic codes. Subscribing under the old name is rejected with "Currency
/// pair not supported", which would silently drop BTC and DOGE.
fn websocket_v2_name(wsname: &str) -> String {
    match wsname.split_once('/') {
        Some(("XBT", quote)) => format!("BTC/{quote}"),
        Some(("XDG", quote)) => format!("DOGE/{quote}"),
        _ => wsname.to_owned(),
    }
}

async fn get<T: serde::de::DeserializeOwned>(http: &reqwest::Client, path: &str) -> Result<T> {
    let response: Envelope<T> = http
        .get(format!("{REST_BASE}{path}"))
        .send()
        .await
        .with_context(|| format!("{path} request failed"))?
        .error_for_status()
        .with_context(|| format!("{path} returned an http error"))?
        .json()
        .await
        .with_context(|| format!("{path} returned malformed json"))?;

    if !response.error.is_empty() {
        bail!("{path}: {}", response.error.join(", "));
    }
    response.result.with_context(|| format!("{path}: no result"))
}
