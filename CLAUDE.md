# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

`util` — the host-side tools for the trading stack. Unlike the other projects
here it is not one program in one language: each tool brings its own
dependencies and is built its own way. See [README.md](README.md).

## Layout

```
feed_sim.py       ZMQ publisher that feeds nqbook a repeated virtual order
md/kraken/        Rust: normalizes Kraken's spot level3 and trade feeds onto the nlib wire and publishes them over ZMQ
third_party/nlib  git submodule: the C++ header the wire records are generated from
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

## The wire format is nlib's, not a copy of it

`nlib` is a submodule at `third_party/nlib`, and `md/kraken/build.rs` runs
bindgen over `third_party/nlib/include/nlib/common.h` to generate the crate's
`nlib` module. The records the publisher sends are the C++ structs, with
bindgen's size, alignment, and offset assertions holding them to the header.
No layout is written down twice, and no `Default` impl exists, so every record
is a struct literal naming every field — a field added to `common.h` reaches
this crate as a compile error, never as frames `nqbook` drops for the wrong
size. Frame tags match `kOrderTag` / `kTradeTag` in `nqbook`'s `Pipeline.h`.

`feed_sim.py`'s `ORDER_FORMAT` is the one hand-written layout left, and
nothing checks it against the header. When `common.h` changes in
[nlib](https://github.com/Zhou-London/nlib), commit there, bump the submodule
(`git -C third_party/nlib fetch && git -C third_party/nlib checkout <sha>`),
and update `ORDER_FORMAT` and its packed-size `assert` in the same change.

Bindgen needs libclang, which comes with the Xcode command line tools.

## A channel is a module

`md/kraken/src/feed.rs` owns everything common to a websocket connection —
connecting, subscribing, stall detection, backoff, publishing — and takes a
`Channel`: the endpoint, whether it needs a token, and the two functions that
build a subscribe request and normalize a message into frames. `level3.rs` and
`trade.rs` are those two channels. A new channel is a third module of that
shape and one more `feed::run` task in `main.rs`, not a branch inside `feed.rs`.

## Credentials

Tools read credentials from the environment or from a file outside the
repository (`md/kraken` uses `~/.config/kraken/credentials`, mode 600). No key,
secret, or token belongs in a source file, a default argument, or a commit.

## Commits

One tool change per commit, imperative subject, body explaining why. Rust build
output under `target/` is gitignored — keep it untracked.
