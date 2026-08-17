// Decodes nlib::metrics — the 120-byte little-endian record nqbook's metrics
// thread publishes — into a plain object. Counters are cumulative since
// process start; consumers difference consecutive samples for rates.

export const METRICS_SIZE = 120;

export interface MetricsSample {
  /** Sample time, Unix-epoch nanoseconds. Rounded through a double, so good
   * to ~microseconds — plenty for rate math and display. */
  tsNs: number;
  feedMessages: number;
  feedBytes: number;
  feedOrders: number;
  feedTrades: number;
  feedDropped: number;
  bookEvents: number;
  bookApplyNs: number;
  bookSamples: number;
  bookInstruments: number;
  bookRestingOrders: number;
  bookMemoryBytes: number;
  writerOrders: number;
  writerTrades: number;
  writerBooks: number;
}

export function decode(frame: Buffer): MetricsSample {
  const u = (offset: number) => Number(frame.readBigUInt64LE(offset));
  return {
    tsNs: Number(frame.readBigInt64LE(0)),
    feedMessages: u(8),
    feedBytes: u(16),
    feedOrders: u(24),
    feedTrades: u(32),
    feedDropped: u(40),
    bookEvents: u(48),
    bookApplyNs: u(56),
    bookSamples: u(64),
    bookInstruments: u(72),
    bookRestingOrders: u(80),
    bookMemoryBytes: u(88),
    writerOrders: u(96),
    writerTrades: u(104),
    writerBooks: u(112),
  };
}
