# kraken

Subscribes to Kraken's spot `level3` (L3 orders) websocket feed for the most
actively traded crypto/USD pairs and discards every message it receives. It
exists to exercise the feed and measure its shape and rate; nothing is
persisted.

```bash
cargo run --release              # top 200 pairs, depth 10, report every 10s
cargo run --release -- --list-symbols
cargo run --release -- --symbols 400 --depth 100 --report-interval 5
```

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

Kraken publishes no popularity ranking, so the program builds one: it reads
`/0/public/AssetPairs` and `/0/public/Ticker`, keeps online pairs quoted in
USD whose base is not itself a fiat currency (which drops FX crosses like
`EUR/USD`), and ranks them by 24-hour volume valued at that day's VWAP. The
top 200 cover about 98% of Kraken's USD volume.

## Connections and rate limits

Kraken caps a websocket connection at 200 symbols, so `--symbols 400` opens
two connections and shards the list across them. Subscriptions are also rate
limited by a counter that a depth-10 symbol increments by 5, depth 100 by 25
and depth 1000 by 100, against a budget of 200 per second on the standard tier
(500 on pro). Subscribe requests are therefore batched to spend at most half
that budget per second, sent from a task separate from the reader so snapshots
arriving mid-subscribe cannot stall the socket.

A connection that errors or goes 20 seconds without a message — Kraken
heartbeats about once a second when idle, so silence means a dead connection —
is dropped and reconnected with capped exponential backoff.

## Reference

- [Level 3 channel](https://docs.kraken.com/api/docs/websocket-v2/level3)
- [GetWebSocketsToken](https://docs.kraken.com/api/docs/rest-api/get-websockets-token)
- [REST authentication](https://docs.kraken.com/api/docs/guides/spot-rest-auth)
