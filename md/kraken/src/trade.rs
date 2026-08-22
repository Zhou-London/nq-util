//! Kraken's public `trade` channel — the executions tape — normalized onto
//! `nlib::trade`.
//!
//! The public tape names no counterparty, so `buy_order_id` and
//! `sell_order_id` stay zero and `side` is the aggressor's; the book stays
//! driven entirely by `level3`, and a trade record is tape data the writer
//! stores alongside it. Prices and quantities are parsed from the raw JSON
//! text into fixed point, as on the order channel. A trade whose fields fail
//! to normalize is skipped and counted.

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::feed::{Channel, Shared, subscribe_reply};
use crate::nlib;
use crate::wire::{self, Frame};

/// The tape is public, so it is served from the general endpoint and
/// subscribed to without a token.
pub const CHANNEL: Channel = Channel {
    url: "wss://ws.kraken.com/v2",
    authenticated: false,
    subscribe,
    normalize,
};

/// `depth` and `token` belong to `level3`; the tape takes neither. Snapshots
/// are declined because they replay trades already published on every
/// reconnect.
fn subscribe(symbols: &[String], _depth: u32, _token: &str) -> String {
    let request = SubscribeRequest {
        method: "subscribe",
        req_id: 2,
        params: SubscribeParams {
            channel: "trade",
            symbol: symbols,
            snapshot: false,
        },
    };
    serde_json::to_string(&request).expect("subscribe request is serializable")
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
    snapshot: bool,
}

/// The fields of an inbound message this channel reads. Prices and quantities
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
    data: Vec<TradeMsg<'a>>,
}

/// Every field is optional so that a message on another channel — the
/// `status` record Kraken opens a connection with — parses and is discarded
/// by the channel check rather than read as a malformed trade.
#[derive(Deserialize)]
struct TradeMsg<'a> {
    #[serde(borrow, default)]
    symbol: Option<&'a str>,
    #[serde(default)]
    side: Option<Aggressor>,
    #[serde(borrow, default)]
    price: Option<&'a RawValue>,
    #[serde(borrow, default)]
    qty: Option<&'a RawValue>,
    #[serde(borrow, default)]
    timestamp: Option<&'a str>,
}

/// Which side took liquidity.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Aggressor {
    Buy,
    Sell,
}

impl From<Aggressor> for nlib::side {
    fn from(aggressor: Aggressor) -> Self {
        match aggressor {
            Aggressor::Buy => nlib::side::buy,
            Aggressor::Sell => nlib::side::sell,
        }
    }
}

fn normalize(text: &str, shared: &Shared, out: &mut Vec<Frame>) {
    let stats = &shared.stats;
    let message: Inbound = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            eprintln!("unparseable trade message: {error}");
            return;
        }
    };

    if subscribe_reply(message.method, message.success, message.error.as_deref(), stats) {
        return;
    }
    if message.channel != Some("trade") {
        return;
    }
    // A snapshot and an update both carry executions, so both append.
    if !matches!(message.kind, Some("snapshot" | "update")) {
        return;
    }

    let mut trades = 0;
    for msg in &message.data {
        let (Some(symbol), Some(side), Some(price), Some(qty), Some(timestamp)) =
            (msg.symbol, msg.side, msg.price, msg.qty, msg.timestamp)
        else {
            stats.note_norm_error();
            continue;
        };
        let normalized = (
            wire::scaled(price.get(), wire::PRICE_DECIMALS),
            wire::scaled(qty.get(), wire::QTY_DECIMALS),
            wire::event_ns(timestamp),
        );
        let (Some(price), Some(qty), Some(event_ns)) = normalized else {
            stats.note_norm_error();
            continue;
        };
        trades += 1;
        out.push(Frame::new(nlib::trade {
            seq: shared.next_seq(),
            // The public tape names no counterparty.
            buy_order_id: 0,
            sell_order_id: 0,
            price,
            qty,
            event_ns,
            instrument_id: wire::instrument_id(symbol),
            side: side.into(),
            // Stamped by the receiving process.
            recv_ns: 0,
        }));
    }
    stats.note_trades(trades);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Kraken opens every connection with a `status` record, which reaches
    /// this channel's parser before the channel check discards it.
    #[test]
    fn a_foreign_channel_message_parses_and_holds_no_trade() {
        let status = r#"{"channel":"status","type":"update","data":[
            {"api_version":"v2","connection_id":1,"system":"online","version":"2.0.11"}]}"#;
        let message: Inbound = serde_json::from_str(status).unwrap();
        assert_eq!(message.channel, Some("status"));
        assert_eq!(message.data.len(), 1);
        assert_eq!(message.data[0].symbol, None);
    }

    #[test]
    fn a_tape_message_carries_every_field() {
        let update = r#"{"channel":"trade","type":"update","data":[
            {"symbol":"BTC/USD","side":"sell","qty":0.5,"price":50000.1,
             "ord_type":"market","trade_id":1,"timestamp":"2023-09-25T07:49:37.708706Z"}]}"#;
        let message: Inbound = serde_json::from_str(update).unwrap();
        let msg = &message.data[0];
        assert_eq!(msg.symbol, Some("BTC/USD"));
        assert!(matches!(msg.side, Some(Aggressor::Sell)));
        assert_eq!(
            wire::scaled(msg.price.unwrap().get(), wire::PRICE_DECIMALS),
            Some(500_001_000_000_000)
        );
        assert_eq!(
            wire::scaled(msg.qty.unwrap().get(), wire::QTY_DECIMALS),
            Some(50_000_000)
        );
        assert_eq!(
            wire::event_ns(msg.timestamp.unwrap()),
            Some(1_695_628_177_708_706_000)
        );
    }
}
