# kraken

Reads Kraken's spot `level3` (L3 orders) websocket feed — every online crypto
pair by default — normalizes each order event onto the nlib wire, and
publishes the framed records on a ZMQ PUB socket for `nqbook`'s feed thread.
Nothing is persisted here; persistence is `nqbook`'s writer stage.

```bash
cargo run --release                    # all pairs, depth 10, PUB on tcp://0.0.0.0:5555
cargo run --release -- --list-symbols  # print instrument_id,symbol and exit
cargo run --release -- --symbols 200 --depth 100 --endpoint tcp://0.0.0.0:6000
```

## Normalization

Every level3 order event becomes one `nlib::order` frame (one tag byte, then
the 72-byte record, little-endian LP64 — the layout `nlib/common.h` pins with
static_asserts and `src/wire.rs` mirrors):

- **Prices and quantities** are parsed from the raw JSON text straight into
  fixed point (`price_scale` 1e10, `qty_scale` 1e8), never through a float.
  Kraken's maximum precision is 10 price and 8 lot decimals, so the
  conversion is exact.
- **Order ids** — base32 strings wider than 64 bits — become their FNV-1a
  hash: stateless, identical across reconnects and restarts. At a million
  resting orders the collision chance is about 5e-8.
- **Symbols** hash to `instrument_id` the same way (32-bit FNV-1a);
  `--list-symbols` prints the mapping for joining stored data back to names.
- **Events** map add → `add`, modify → `modify` (new remaining quantity,
  price unchanged, queue priority kept), delete → `cancel` (the order leaves
  the book). A snapshot replays as a `clear` for the instrument followed by
  an `add` per resting order, so a reconnect cannot leave ghost orders in a
  downstream book.
- **Times**: each event carries Kraken's RFC3339 timestamp as `event_ns`;
  `recv_ns` is left zero on the wire and stamped by the receiving process.
- `seq` is one process-wide counter across all connections, so the ZMQ
  stream is a single ordered feed.

An order whose fields fail to normalize is dropped and counted (`normalize
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

All online spot pairs offered over websocket whose base is a cryptoasset —
about 1330 — over every quote currency; FX crosses like `EUR/USD` are
excluded. `--symbols N` keeps the N most active, ranked by 24-hour trade
count from `/0/public/Ticker`: unlike volume, which is priced in each pair's
own quote currency, a trade count compares across quotes.

## Connections and rate limits

Kraken caps a websocket connection at 200 symbols, so the full list shards
across seven connections. Subscriptions are rate limited by a counter that a
depth-10 symbol increments by 5, depth 100 by 25 and depth 1000 by 100,
against a budget of 200 per second on the standard tier (500 on pro) — and
the budget belongs to the account, not the connection. Subscribe requests are
therefore batched to cost at most half that budget and paced through one
global one-batch-per-second gate shared by every connection, sent from a task
separate from the reader so snapshots arriving mid-subscribe cannot stall the
socket. The full subscription takes about 70 seconds to spread out.

A connection that errors or goes 20 seconds without a message — Kraken
heartbeats about once a second when idle, so silence means a dead connection —
is dropped and reconnected with capped exponential backoff, resubscribing
through the same gate and replaying fresh snapshots.

## Reference

- [Level 3 channel](https://docs.kraken.com/api/docs/websocket-v2/level3)
- [GetWebSocketsToken](https://docs.kraken.com/api/docs/rest-api/get-websockets-token)
- [REST authentication](https://docs.kraken.com/api/docs/guides/spot-rest-auth)
