# kraken

Reads Kraken's spot `level3` (L3 orders) and `trade` (the executions tape)
websocket feeds for the most actively traded crypto pairs — 35 by default, one
connection per channel — normalizes every event onto an `nlib::order` or
`nlib::trade`, and publishes the framed records on one ZMQ PUB socket for
`nqbook`'s feed thread. Nothing is persisted here; persistence is `nqbook`'s
writer stage.

Both connections draw sequence numbers from one counter and hand frames to the
same socket, so `nqbook` reads a single ordered stream. `--symbols` and the
ranking apply to both; `--depth` is `level3`'s alone.

```bash
cargo run --release                    # top 35 pairs, depth 10, PUB on tcp://0.0.0.0:5555
cargo run --release -- --list-symbols  # print instrument_id,symbol and exit
cargo run --release -- --symbols 40 --endpoint tcp://0.0.0.0:6000
```

## The wire records

A frame is one tag byte — 0 for an order, 1 for a trade, matching `kOrderTag`
and `kTradeTag` in `nqbook`'s `Pipeline.h` — followed by the record's own
bytes in host layout.

The records are not written down here. `build.rs` runs bindgen over
`../../third_party/nlib/include/nlib/common.h`, so `src/nlib.rs` is the C++
structs themselves: bindgen emits `#[repr(C)]` with compile-time size,
alignment and offset assertions, and no `Default` impl, so every record is a
struct literal naming every field. A field added to `common.h` is a compile
error in this crate rather than frames `nqbook` drops for the wrong size.
Building therefore needs libclang, which ships with the Xcode command line
tools.

## Normalization

Every level3 order event becomes one `nlib::order` frame:

- **Prices and quantities** are parsed from the raw JSON text straight into
  fixed point (`price_scale` 1e10, `qty_scale` 1e8), never through a float.
  Kraken's maximum precision is 10 price and 8 lot decimals, so the
  conversion is exact.
- **Order ids** — base32 strings wider than 64 bits — become their FNV-1a
  hash: stateless, identical across reconnects and restarts. At a million
  resting orders the collision chance is about 5e-8.
- **Symbols** hash to `instrument_id` the same way (32-bit FNV-1a);
  `--list-symbols` prints the mapping for joining stored data back to names.
- **Events** map add → `add`, modify → `modify`, delete → `cancel`. Kraken
  reports one quantity per event and the action says which field it fills: a
  modify's new remaining quantity goes in `new_qty` (price unchanged, queue
  priority kept), a delete's departing quantity in `cancel_qty`, leaving `qty`
  meaning the resting quantity of an add. A snapshot replays as a `clear` for
  the instrument followed by an `add` per resting order, so a reconnect cannot
  leave ghost orders in a downstream book.
- **Times**: each event carries Kraken's RFC3339 timestamp as `event_ns`;
  `recv_ns` is left zero on the wire and stamped by the receiving process.
- `seq` is one process-wide counter across all connections, so the ZMQ
  stream is a single ordered feed.

Every trade on the tape becomes one `nlib::trade` frame, with prices, times,
and hashes converted the same way. The public tape names no counterparty, so
`buy_order_id` and `sell_order_id` stay zero and `side` is the aggressor's —
the book is driven entirely by `level3`, and trades are tape data the writer
stores alongside it.

A record whose fields fail to normalize is dropped and counted (`normalize
errors` in the report line), never sent half-formed.

The publisher is the pure-Rust `zeromq` crate speaking ZMTP 3.0 — libzmq
subscribers (`nqbook`, pyzmq) interoperate, and `cargo build` needs no system
libzmq. PUB semantics: no subscriber, frames drop; slow subscriber, frames
drop at the high-water mark rather than stalling the feed.

## Credentials

`level3` is an authenticated channel, so the program needs a Kraken Spot API
key pair. It reads `KRAKEN_API_KEY` and `KRAKEN_API_SECRET`, falling back to a
file at `KRAKEN_CREDENTIALS` or `~/.config/kraken/credentials`:

```
key = <api key>
secret = <private key, base64 as Kraken shows it>
```

Keep that file mode 600. No credential belongs in this repository.

The key needs no trading permission — only the ability to call
`GetWebSocketsToken`, which every key has. The program signs a REST call for a
websockets token (base64(HMAC-SHA512(uri_path || SHA256(nonce || body)))) and
passes the token in each subscribe request. A token must be used within 15
minutes but does not expire once a subscription is established, so a fresh one
is fetched per connection attempt.

## Which symbols

The whole market is ranked — every online spot pair offered over websocket
whose base is a cryptoasset (about 1330; FX crosses like `EUR/USD` are
excluded), by 24-hour trade count from `/0/public/Ticker`, which unlike
volume compares across quote currencies — and the subscription takes the top
`--symbols` of it.

## Adding a channel

`src/feed.rs` owns everything a connection does — connecting, subscribing,
stall detection, backoff, publishing — and takes a `Channel`: the endpoint,
whether it needs a websockets token, and the two functions that build a
subscribe request and normalize a message into frames. `src/level3.rs` and
`src/trade.rs` are those two channels; a third is another module of that shape
plus one more `feed::run` task in `main.rs`.

## The connections and rate limits

One websocket connection per channel, one subscribe request each. `level3`
lives on its own endpoint (`ws-l3.kraken.com`) and is authenticated; the tape
is public and served from `ws.kraken.com`. Subscriptions are rate
limited account-wide by a counter that a depth-10 symbol increments by 5
(depth 100 by 25, depth 1000 by 100) against a budget of 200 per second on
the standard tier, 500 on pro. That budget is what caps `--symbols`: the
default 35 costs 175 and fits the standard tier in one request; a pro tier
fits 100; Kraken caps a connection at 200 symbols regardless. Raise the flag
as the account's tier allows.

A connection that errors or goes 20 seconds without a message — Kraken
heartbeats about once a second when idle, so silence means a dead connection —
is dropped and reconnected with capped exponential backoff, independently of
the other channel. `level3` replays a fresh snapshot per symbol on reconnect;
the tape declines snapshots, so it resumes without republishing trades.

## Reference

- [Level 3 channel](https://docs.kraken.com/api/docs/websocket-v2/level3)
- [Trade channel](https://docs.kraken.com/api/docs/websocket-v2/trade)
- [GetWebSocketsToken](https://docs.kraken.com/api/docs/rest-api/get-websockets-token)
- [REST authentication](https://docs.kraken.com/api/docs/guides/spot-rest-auth)
