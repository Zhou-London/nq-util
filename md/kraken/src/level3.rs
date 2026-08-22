//! Kraken's `level3` channel — the order-by-order view of the book —
//! normalized onto `nlib::order`.
//!
//! Bids are buys and asks sells; a snapshot replays as a `clear` followed by
//! an `add` per resting order. An update maps add -> `add`, modify ->
//! `modify` carrying the new remaining quantity in `new_qty`, and delete ->
//! `cancel` carrying the quantity leaving the book in `cancel_qty`. Prices
//! and quantities are parsed from the raw JSON text into fixed point, ids and
//! symbols hash to the wire's integer ids. An order whose fields fail to
//! normalize is skipped and counted, never sent half-formed.

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::feed::{Channel, Shared, subscribe_reply};
use crate::nlib;
use crate::wire::{self, Frame};

/// `level3` lives on its own endpoint, not the general authenticated one.
pub const CHANNEL: Channel = Channel {
    url: "wss://ws-l3.kraken.com/v2",
    authenticated: true,
    subscribe,
    normalize,
};

fn subscribe(symbols: &[String], depth: u32, token: &str) -> String {
    let request = SubscribeRequest {
        method: "subscribe",
        req_id: 1,
        params: SubscribeParams {
            channel: "level3",
            symbol: symbols,
            depth,
            snapshot: true,
            token,
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
    depth: u32,
    snapshot: bool,
    token: &'a str,
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

fn normalize(text: &str, shared: &Shared, out: &mut Vec<Frame>) {
    let stats = &shared.stats;
    let message: Inbound = match serde_json::from_str(text) {
        Ok(message) => message,
        Err(error) => {
            eprintln!("unparseable level3 message: {error}");
            return;
        }
    };

    if subscribe_reply(message.method, message.success, message.error.as_deref(), stats) {
        return;
    }
    if message.channel != Some("level3") {
        return;
    }
    let snapshot = match message.kind {
        Some("snapshot") => true,
        Some("update") => false,
        _ => return,
    };

    for book in &message.data {
        let Some(symbol) = book.symbol else {
            stats.note_norm_error();
            continue;
        };
        let instrument_id = wire::instrument_id(symbol);

        // Normalize the whole message before emitting any of it: a snapshot's
        // clear carries the newest event time its orders replay up to, and it
        // takes the sequence number ahead of them.
        let mut orders = Vec::with_capacity(book.bids.len() + book.asks.len());
        let (mut adds, mut modifies, mut deletes) = (0, 0, 0);
        for (side, msgs) in [
            (nlib::side::buy, &book.bids),
            (nlib::side::sell, &book.asks),
        ] {
            for msg in msgs {
                let action = match (snapshot, msg.event) {
                    (true, _) => nlib::order_action::add,
                    (false, Some(EventKind::Add)) => nlib::order_action::add,
                    (false, Some(EventKind::Modify)) => nlib::order_action::modify,
                    (false, Some(EventKind::Delete)) => nlib::order_action::cancel,
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
                // Kraken reports one quantity per event; the action says
                // which field it fills. A delete takes the whole remaining
                // quantity out of the book, a modify replaces it.
                let (cancel_qty, new_qty) = match action {
                    nlib::order_action::cancel => (qty, 0),
                    nlib::order_action::modify => (0, qty),
                    _ => (0, 0),
                };
                match action {
                    nlib::order_action::add => adds += 1,
                    nlib::order_action::modify => modifies += 1,
                    _ => deletes += 1,
                }
                orders.push(nlib::order {
                    seq: 0, // stamped at emit
                    order_id: wire::order_id(msg.order_id),
                    price,
                    qty,
                    cancel_qty,
                    new_qty,
                    event_ns,
                    // The list hooks belong to the book that rests the order,
                    // and the receiving process stamps recv_ns.
                    prev: std::ptr::null_mut(),
                    next: std::ptr::null_mut(),
                    instrument_id,
                    side,
                    // Only limit orders rest in a level3 book.
                    type_: nlib::order_type::limit,
                    action,
                    recv_ns: 0,
                });
            }
        }

        if snapshot {
            let event_ns = orders.iter().map(|o| o.event_ns).max().unwrap_or(0);
            // A clear drops the instrument's resting orders; the book reads
            // the action, the instrument and the times.
            emit(
                shared,
                out,
                nlib::order {
                    seq: 0,
                    order_id: 0,
                    price: 0,
                    qty: 0,
                    cancel_qty: 0,
                    new_qty: 0,
                    event_ns,
                    prev: std::ptr::null_mut(),
                    next: std::ptr::null_mut(),
                    instrument_id,
                    side: nlib::side::buy,
                    type_: nlib::order_type::limit,
                    action: nlib::order_action::clear,
                    recv_ns: 0,
                },
            );
            stats.note_snapshot(orders.len() as u64);
        } else {
            stats.note_update(adds, modifies, deletes);
        }
        for record in orders {
            emit(shared, out, record);
        }
    }
}

/// Stamps `record` with the next sequence number and frames it.
fn emit(shared: &Shared, out: &mut Vec<Frame>, mut record: nlib::order) {
    record.seq = shared.next_seq();
    out.push(Frame::new(record));
}
