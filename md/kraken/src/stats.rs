//! Counters over the discarded feed, shared by every connection.

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

    /// Records one update message and tallies its order events by kind.
    pub fn note_update(&self, events: impl Iterator<Item = Option<EventKind>>) {
        let (mut adds, mut modifies, mut deletes, mut total) = (0, 0, 0, 0);
        for event in events {
            total += 1;
            match event {
                Some(EventKind::Add) => adds += 1,
                Some(EventKind::Modify) => modifies += 1,
                Some(EventKind::Delete) => deletes += 1,
                None => {}
            }
        }
        self.updates.fetch_add(1, Ordering::Relaxed);
        self.orders.fetch_add(total, Ordering::Relaxed);
        self.adds.fetch_add(adds, Ordering::Relaxed);
        self.modifies.fetch_add(modifies, Ordering::Relaxed);
        self.deletes.fetch_add(deletes, Ordering::Relaxed);
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
            disconnects: self.disconnects.load(Ordering::Relaxed),
            subscribe_failures: self.subscribe_failures.load(Ordering::Relaxed),
        }
    }
}

/// What an order in a `level3` update did. Absent in a snapshot, where every
/// order is simply present.
#[derive(serde::Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Add,
    Modify,
    Delete,
}

impl Snapshot {
    /// Formats the change since `previous` over `elapsed` as one line.
    pub fn report(&self, previous: &Snapshot, elapsed: Duration) -> String {
        let secs = elapsed.as_secs_f64().max(f64::EPSILON);
        let updates = self.updates - previous.updates;
        let orders = self.orders - previous.orders;
        let bytes = self.bytes - previous.bytes;

        let mut line = format!(
            "{:>8.0} msg/s  {:>9.0} order/s  {:>7.2} MB/s  |  \
             total {} snapshots, {} updates, {} orders \
             (+{} add, ~{} mod, -{} del)",
            updates as f64 / secs,
            orders as f64 / secs,
            bytes as f64 / secs / (1 << 20) as f64,
            self.snapshots,
            self.updates,
            self.orders,
            self.adds,
            self.modifies,
            self.deletes,
        );
        if self.disconnects > 0 || self.subscribe_failures > 0 {
            line.push_str(&format!(
                "  |  {} disconnects, {} subscribe failures",
                self.disconnects, self.subscribe_failures
            ));
        }
        line
    }
}
