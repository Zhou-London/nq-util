<img src="https://capsule-render.vercel.app/api?type=waving&height=400&text=Util&fontAlign=80&fontAlignY=40&color=gradient" />

<p align="center">
  <img alt="Python 3.12+" src="https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white" />
  <img alt="uv" src="https://img.shields.io/badge/run%20with-uv-DE5FE9?logo=uv&logoColor=white" />
  <img alt="Rust 2024" src="https://img.shields.io/badge/Rust-2024-000000?logo=rust&logoColor=white" />
  <img alt="Runs on the host" src="https://img.shields.io/badge/runs-on%20the%20host-4c1" />
</p>

Project of NowQuant.

Host-side tools for the trading stack, in whatever language suits the tool.
Each one brings its own dependencies; there is no shared project file. These
run on the host — the `dev` container is for the services they talk to.

## Python

Scripts run with [uv](https://docs.astral.sh/uv/), each carrying its own PEP
723 dependency block.

### feed_sim.py

Publishes one repeated virtual order to the `nqbook` process over ZMQ.

```bash
uv run feed_sim.py                 # PUB on tcp://*:5555, one order per 100 ms
uv run feed_sim.py --interval 0.5  # slower
```

`nqbook` runs in docker and connects to `tcp://host.docker.internal:5555` by
default, which reaches this publisher on the host.

## Rust

Cargo packages, built with the host toolchain (`rustup`, keg-only under
Homebrew — `/opt/homebrew/opt/rustup/bin` must be on `PATH`).

### md/kraken

Reads Kraken's spot `level3` (L3 orders) websocket feed — every online crypto
pair by default — normalizes each order event onto the nlib wire, and
publishes the framed records over ZMQ for `nqbook`'s feed thread. Needs a
Kraken API key pair; see [md/kraken/README.md](md/kraken/README.md).

```bash
cd md/kraken && cargo run --release   # PUB on tcp://0.0.0.0:5555
```

## Releases

### 2026-08-16

The repository was split out of `nqbook` as the place for host-side tools, and
took its first two.

- **`feed_sim.py`** — publishes a repeated virtual order framed exactly as
  `nqbook`'s `RunFeed` expects (tag byte plus `nlib::order` host layout), so
  the service can be run end to end without a real feed. The layout is
  hand-written and unchecked: a field added to `nlib::order` must be mirrored
  here or every frame is silently dropped as the wrong size.
- **`md/kraken`** — reads Kraken's authenticated spot `level3` websocket feed
  and discards it, to measure the feed's shape and rate. It builds its own
  popularity ranking from `AssetPairs` and `Ticker` (Kraken publishes none),
  shards symbols across connections at Kraken's 200-per-connection cap, spends
  at most half the subscribe rate budget per second, and reconnects with capped
  backoff when a connection errors or goes quiet for 20 seconds.
- Credentials stay outside the repository — environment variables, or a
  mode-600 file under `~/.config`.
