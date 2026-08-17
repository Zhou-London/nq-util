//! Counters over the normalized feed, shared by every connection and the
//! publisher.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Stats {
    snapshots: AtomicU64,
    updates: AtomicU64,
    adds: AtomicU64,
    modifies: AtomicU64,
    deletes: AtomicU64,
    orders: AtomicU64,
    bytes: AtomicU64,
    published: AtomicU64,
    norm_errors: AtomicU64,
    disconnects: AtomicU64,
    subscribe_failures: AtomicU64,
}

/// Counter totals at one instant.
pub struct Snapshot {
    pub snapshots: u64,
    pub updates: u64,
    pub adds: u64,
    pub modifies: u64,
    pub deletes: u64,
    pub orders: u64,
    pub bytes: u64,
    pub published: u64,
    pub norm_errors: u64,
    pub disconnects: u64,
    pub subscribe_failures: u64,
}

impl Stats {
    pub fn note_bytes(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn note_snapshot(&self, orders: u64) {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        self.orders.fetch_add(orders, Ordering::Relaxed);
    }

    /// Records one update message and its order events by kind.
    pub fn note_update(&self, adds: u64, modifies: u64, deletes: u64) {
        self.updates.fetch_add(1, Ordering::Relaxed);
        self.orders.fetch_add(adds + modifies + deletes, Ordering::Relaxed);
        self.adds.fetch_add(adds, Ordering::Relaxed);
        self.modifies.fetch_add(modifies, Ordering::Relaxed);
        self.deletes.fetch_add(deletes, Ordering::Relaxed);
    }

    /// Records one frame handed to the PUB socket.
    pub fn note_published(&self) {
        self.published.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one order event dropped because a field failed to normalize.
    pub fn note_norm_error(&self) {
        self.norm_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_disconnect(&self) {
        self.disconnects.fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_subscribe_failure(&self) {
        self.subscribe_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            snapshots: self.snapshots.load(Ordering::Relaxed),
            updates: self.updates.load(Ordering::Relaxed),
            adds: self.adds.load(Ordering::Relaxed),
            modifies: self.modifies.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            orders: self.orders.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            published: self.published.load(Ordering::Relaxed),
            norm_errors: self.norm_errors.load(Ordering::Relaxed),
            disconnects: self.disconnects.load(Ordering::Relaxed),
            subscribe_failures: self.subscribe_failures.load(Ordering::Relaxed),
        }
    }
}

impl Snapshot {
    /// Formats the change since `previous` over `elapsed` as one line.
    pub fn report(&self, previous: &Snapshot, elapsed: Duration) -> String {
        let secs = elapsed.as_secs_f64().max(f64::EPSILON);
        let updates = self.updates - previous.updates;
        let orders = self.orders - previous.orders;
        let published = self.published - previous.published;
        let bytes = self.bytes - previous.bytes;

        let mut line = format!(
            "{:>8.0} msg/s  {:>9.0} order/s  {:>9.0} pub/s  {:>7.2} MB/s  |  \
             total {} snapshots, {} updates, {} orders \
             (+{} add, ~{} mod, -{} del), {} published",
            updates as f64 / secs,
            orders as f64 / secs,
            published as f64 / secs,
            bytes as f64 / secs / (1 << 20) as f64,
            self.snapshots,
            self.updates,
            self.orders,
            self.adds,
            self.modifies,
            self.deletes,
            self.published,
        );
        if self.disconnects > 0 || self.subscribe_failures > 0 || self.norm_errors > 0 {
            line.push_str(&format!(
                "  |  {} disconnects, {} subscribe failures, {} normalize errors",
                self.disconnects, self.subscribe_failures, self.norm_errors
            ));
        }
        line
    }
}
