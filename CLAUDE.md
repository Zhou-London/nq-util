# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

`util` — the host-side tools for the trading stack. Unlike the other projects
here it is not one program in one language: each tool brings its own
dependencies and is built its own way. See [README.md](README.md).

## Layout

```
feed_sim.py       ZMQ publisher that feeds nqbook a repeated virtual order
md/kraken/        Rust: reads Kraken's spot level3 websocket feed and discards it
```

Everything here runs on the **host**, not in the `dev` container — that is what
separates this repository from `nlib` and `orderbook`. A tool that has to run
in a container belongs with the service it serves.

## Python

Scripts are single files with a PEP 723 dependency block and are run with
`uv run <script>.py`. There is no `pyproject.toml`, no lockfile, and no shared
virtualenv; a script that needs a dependency declares it in its own header.

## Rust

Each Cargo package is its own crate under a category directory (`md/` for
market data). The host toolchain comes from `rustup`, which Homebrew installs
keg-only — `/opt/homebrew/opt/rustup/bin` must be on `PATH` for `cargo` to
resolve.

```bash
cd md/kraken && cargo build --release
```

Module-level `//!` comments carry the contract: what the module is responsible
for, and the protocol or rate-limit rule it encodes. Keep them accurate when
the behavior changes — they are the documentation.

## feed_sim.py encodes nqbook's wire format

`ORDER_FORMAT` is a hand-written `struct` layout of `nlib::order`, and the tag
byte matches `kOrderTag` in `nqbook`'s `Pipeline.h`. Nothing checks the two
against each other: a field added to `nlib::order` makes the simulator send
frames that `RunFeed` silently drops as the wrong size.

So when `common.h` changes in [nlib](https://github.com/Zhou-London/nlib),
update `ORDER_FORMAT` and its `assert` on the packed size in the same change.

## Credentials

Tools read credentials from the environment or from a file outside the
repository (`md/kraken` uses `~/.config/kraken/credentials`, mode 600). No key,
secret, or token belongs in a source file, a default argument, or a commit.

## Commits

One tool change per commit, imperative subject, body explaining why. Rust build
output under `target/` is gitignored — keep it untracked.
