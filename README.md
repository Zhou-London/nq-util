# util

Host-side Python utilities for the trading stack, run with [uv](https://docs.astral.sh/uv/).
Each script carries its own PEP 723 dependency block; no shared project file.

## feed_sim.py

Publishes one repeated virtual order to the `nqbook` process over ZMQ.

```bash
uv run feed_sim.py                 # PUB on tcp://*:5555, one order per 100 ms
uv run feed_sim.py --interval 0.5  # slower
```

`nqbook` runs in docker and connects to `tcp://host.docker.internal:5555` by
default, which reaches this publisher on the host.
