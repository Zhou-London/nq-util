//! Ranks every online spot pair whose base is a cryptoasset, over any quote
//! currency, by activity; the subscription takes the head of that ranking.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

const REST_BASE: &str = "https://api.kraken.com";

/// Kraken's classic asset codes prefix fiat with `Z` (`ZUSD`, `ZEUR`). Pairs
/// whose base is fiat are FX crosses, not crypto, so they are excluded.
const FIAT_PREFIX: char = 'Z';

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
    status: String,
}

#[derive(Deserialize)]
struct Ticker {
    /// `[today, last 24 hours]` trade count. Unlike volume, which is priced
    /// in each pair's own quote currency, a trade count compares across
    /// quotes, so one ranking can cover the whole exchange.
    t: [u64; 2],
}

/// Returns the `limit` most active online crypto spot pairs as websocket
/// names, most actively traded first by 24-hour trade count. The whole
/// market is ranked; `limit` only truncates.
pub async fn select(http: &reqwest::Client, limit: usize) -> Result<Vec<String>> {
    let pairs: HashMap<String, AssetPair> = get(http, "/0/public/AssetPairs").await?;
    let tickers: HashMap<String, Ticker> = get(http, "/0/public/Ticker").await?;

    let mut ranked: Vec<(u64, String)> = pairs
        .iter()
        .filter(|(_, pair)| {
            pair.status == "online"
                && !pair.base.starts_with(FIAT_PREFIX)
                && pair.wsname.is_some()
        })
        .map(|(id, pair)| {
            let trades = tickers.get(id).map_or(0, |ticker| ticker.t[1]);
            let name = websocket_v2_name(pair.wsname.as_deref().expect("filtered above"));
            (trades, name)
        })
        .collect();

    if ranked.is_empty() {
        bail!("no tradeable pairs found");
    }
    ranked.sort_unstable_by(|a, b| b.cmp(a));
    ranked.truncate(limit);
    Ok(ranked.into_iter().map(|(_, name)| name).collect())
}

/// Websocket v2 renamed two assets that `wsname` still reports under their
/// classic codes, on either side of the pair. Subscribing under an old name
/// is rejected with "Currency pair not supported", which would silently drop
/// BTC and DOGE pairs and every BTC-quoted book.
fn websocket_v2_name(wsname: &str) -> String {
    fn asset(code: &str) -> &str {
        match code {
            "XBT" => "BTC",
            "XDG" => "DOGE",
            _ => code,
        }
    }
    match wsname.split_once('/') {
        Some((base, quote)) => format!("{}/{}", asset(base), asset(quote)),
        None => wsname.to_owned(),
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
