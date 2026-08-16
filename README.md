# util

Host-side tools for the trading stack, in whatever language suits the tool.
Each one brings its own dependencies; there is no shared project file.

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

Subscribes to Kraken's spot `level3` (L3 orders) websocket feed for the most
actively traded crypto/USD pairs and discards everything it receives, to
exercise the feed and measure its rate. Needs a Kraken API key pair; see
[md/kraken/README.md](md/kraken/README.md).

```bash
cd md/kraken && cargo run --release
```
