//! Binds one ZMQ PUB socket and publishes every framed record to it, in
//! channel order. The socket is the pure-Rust `zeromq` crate speaking ZMTP
//! 3.0, so nqbook's libzmq SUB interoperates while `cargo build` needs no
//! system libzmq. PUB semantics apply: frames sent with no subscriber
//! connected are dropped, and a slow subscriber drops frames at the socket's
//! high-water mark rather than backpressuring the feed.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use zeromq::{Socket, SocketSend, ZmqMessage};

use crate::feed::Shared;
use crate::wire::Frame;

/// Publishes frames from `rx` on `endpoint` until every sender is dropped or
/// the socket fails. The connections hold senders for the life of the
/// process, so returning at all means the pipeline is over.
pub async fn run(endpoint: String, mut rx: mpsc::Receiver<Frame>, shared: Arc<Shared>) -> Result<()> {
    let mut socket = zeromq::PubSocket::new();
    socket
        .bind(&endpoint)
        .await
        .with_context(|| format!("bind {endpoint}"))?;
    eprintln!("publishing on {endpoint}");

    while let Some(frame) = rx.recv().await {
        socket
            .send(ZmqMessage::from(frame.as_bytes().to_vec()))
            .await
            .context("zmq send failed")?;
        shared.stats.note_published();
    }
    Ok(())
}
