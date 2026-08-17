"use client";

// The dashboard: subscribes to /api/metrics (SSE), keeps a rolling 60 s of
// samples, and differences consecutive cumulative counters into rates on the
// client. All series wear the one accent hue; status colors mark only the
// connection state and the dropped counter.

import { useEffect, useMemo, useRef, useState } from "react";

import type { MetricsSample } from "@/lib/metrics";

/** Samples kept: 60 s at the publisher's 10 Hz. */
const WINDOW = 600;

/** Trailing samples for the apply-latency estimate; applies are timed 1 in
 * 1024, so a single 100 ms window usually holds no timed apply. */
const LATENCY_LOOKBACK = 50;

type Status = "connecting" | "live" | "stalled" | "offline";

function useMetricsStream() {
  const [samples, setSamples] = useState<MetricsSample[]>([]);
  const [status, setStatus] = useState<Status>("connecting");
  const [endpoint, setEndpoint] = useState("");
  const lastArrival = useRef(0);

  useEffect(() => {
    const source = new EventSource("/api/metrics");
    source.addEventListener("hello", (event) => {
      setEndpoint(JSON.parse((event as MessageEvent).data).endpoint);
    });
    source.onmessage = (event) => {
      const sample: MetricsSample = JSON.parse(event.data);
      lastArrival.current = Date.now();
      setStatus("live");
      setSamples((prev) => {
        // A counter running backwards means nqbook restarted; the old
        // window would difference into nonsense, so start over.
        const last = prev[prev.length - 1];
        if (last && sample.feedMessages < last.feedMessages) return [sample];
        return [...prev.slice(-(WINDOW - 1)), sample];
      });
    };
    source.onerror = () => setStatus("offline");

    // The publisher ticks every 100 ms; a second of silence means the
    // pipeline stopped even if the SSE socket is still up.
    const stale = setInterval(() => {
      setStatus((current) =>
        current === "live" && Date.now() - lastArrival.current > 1500 ? "stalled" : current,
      );
    }, 500);
    return () => {
      clearInterval(stale);
      source.close();
    };
  }, []);

  return { samples, status, endpoint };
}

/** Per-second rate of a cumulative counter, one point per adjacent pair. */
function rateSeries(samples: MetricsSample[], value: (s: MetricsSample) => number): number[] {
  const out: number[] = [];
  for (let i = 1; i < samples.length; i++) {
    const dt = (samples[i].tsNs - samples[i - 1].tsNs) / 1e9;
    out.push(dt > 0 ? (value(samples[i]) - value(samples[i - 1])) / dt : 0);
  }
  return out;
}

/** Average timed-apply latency over a trailing window, per sample. */
function latencySeries(samples: MetricsSample[]): number[] {
  const out: number[] = [];
  for (let i = 1; i < samples.length; i++) {
    const j = Math.max(0, i - LATENCY_LOOKBACK);
    const timed = samples[i].bookSamples - samples[j].bookSamples;
    out.push(
      timed > 0
        ? (samples[i].bookApplyNs - samples[j].bookApplyNs) / timed
        : samples[i].bookSamples > 0
          ? samples[i].bookApplyNs / samples[i].bookSamples
          : 0,
    );
  }
  return out;
}

function gaugeSeries(samples: MetricsSample[], key: keyof MetricsSample): number[] {
  return samples.slice(1).map((sample) => sample[key]);
}

const fmtCount = (n: number): string => {
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e4) return `${(n / 1e3).toFixed(1)}k`;
  return n >= 100 || Number.isInteger(n) ? n.toFixed(0) : n.toFixed(1);
};

const fmtBytes = (n: number): string => {
  if (n >= 1 << 30) return `${(n / (1 << 30)).toFixed(2)} GB`;
  if (n >= 1 << 20) return `${(n / (1 << 20)).toFixed(2)} MB`;
  if (n >= 1 << 10) return `${(n / (1 << 10)).toFixed(1)} KB`;
  return `${n.toFixed(0)} B`;
};

const fmtNs = (n: number): string => {
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)} ms`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} µs`;
  return `${n.toFixed(0)} ns`;
};

// A single-series line with a hover crosshair. Points are drawn oldest to
// newest across the full width; the y-scale is 0-or-min to max with a little
// headroom, so a flat series still reads as a line, not an artifact.
function Sparkline({
  points,
  height = 44,
  format,
}: {
  points: number[];
  height?: number;
  format: (v: number) => string;
}) {
  const [hover, setHover] = useState<number | null>(null);
  const width = 100; // viewBox units; the SVG stretches to the tile

  const { path, area, min, max, ys } = useMemo(() => {
    if (points.length < 2) return { path: "", area: "", min: 0, max: 0, ys: [] as number[] };
    const lo = Math.min(0, ...points);
    const hi = Math.max(...points);
    const span = hi - lo || 1;
    const pad = span * 0.08;
    const y = (v: number) => height - 3 - ((v - lo + pad * 0.5) / (span + pad)) * (height - 6);
    const x = (i: number) => (i / (points.length - 1)) * width;
    const ys = points.map(y);
    const path = points.map((_, i) => `${i ? "L" : "M"}${x(i).toFixed(2)},${ys[i].toFixed(2)}`).join("");
    const area = `${path}L${width},${height}L0,${height}Z`;
    return { path, area, min: lo, max: hi, ys };
  }, [points, height]);

  if (points.length < 2) return <div style={{ height }} />;

  const index = hover === null ? null : Math.min(points.length - 1, Math.max(0, hover));
  return (
    <div
      className="spark"
      style={{ height }}
      onMouseLeave={() => setHover(null)}
      onMouseMove={(event) => {
        const rect = event.currentTarget.getBoundingClientRect();
        setHover(Math.round(((event.clientX - rect.left) / rect.width) * (points.length - 1)));
      }}
    >
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        style={{ display: "block", width: "100%", height }}
        aria-label={`last minute, min ${format(min)}, max ${format(max)}`}
      >
        <path d={area} fill="var(--accent-wash)" opacity={0.45} />
        <path d={path} fill="none" stroke="var(--accent)" strokeWidth={1.4} vectorEffect="non-scaling-stroke" />
        {index !== null && (
          <line
            x1={(index / (points.length - 1)) * width}
            x2={(index / (points.length - 1)) * width}
            y1={0}
            y2={height}
            stroke="var(--ink-3)"
            strokeWidth={1}
            vectorEffect="non-scaling-stroke"
          />
        )}
      </svg>
      {index !== null && (
        <>
          <div
            style={{
              position: "absolute",
              left: `${(index / (points.length - 1)) * 100}%`,
              top: `${(ys[index] / height) * 100}%`,
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: "var(--accent)",
              border: "2px solid var(--card)",
              transform: "translate(-50%, -50%)",
              pointerEvents: "none",
            }}
          />
          <div
            className="tooltip"
            style={{ left: `${(index / (points.length - 1)) * 100}%`, top: -4 }}
          >
            {format(points[index])} · {((points.length - 1 - index) / 10).toFixed(1)}s ago
          </div>
        </>
      )}
    </div>
  );
}

function Tile({
  label,
  points,
  format,
  unit,
  wide = false,
  height,
  smooth = false,
}: {
  label: string;
  points: number[];
  format: (v: number) => string;
  unit?: string;
  wide?: boolean;
  height?: number;
  /** Headline as the mean of the last second; rates at 100 ms granularity
   * flicker through zero, and the sparkline already shows the raw shape. */
  smooth?: boolean;
}) {
  let latest: number | null = points.length ? points[points.length - 1] : null;
  if (smooth && points.length) {
    const tail = points.slice(-10);
    latest = tail.reduce((a, b) => a + b, 0) / tail.length;
  }
  return (
    <div className={wide ? "tile wide" : "tile"}>
      <div className="label">{label}</div>
      <div className="value">
        {latest === null ? "—" : format(latest)}
        {unit && <span className="unit">{unit}</span>}
      </div>
      <Sparkline points={points} format={format} height={height ?? (wide ? 120 : 44)} />
    </div>
  );
}

const STATUS_TEXT: Record<Status, string> = {
  connecting: "connecting…",
  live: "live",
  stalled: "stream stalled",
  offline: "bridge offline",
};

export default function Page() {
  const { samples, status, endpoint } = useMetricsStream();

  const series = useMemo(
    () => ({
      feedMsg: rateSeries(samples, (s) => s.feedMessages),
      feedBytes: rateSeries(samples, (s) => s.feedBytes),
      bookEvents: rateSeries(samples, (s) => s.bookEvents),
      writerRows: rateSeries(samples, (s) => s.writerOrders + s.writerTrades + s.writerBooks),
      latency: latencySeries(samples),
      resting: gaugeSeries(samples, "bookRestingOrders"),
      instruments: gaugeSeries(samples, "bookInstruments"),
      memory: gaugeSeries(samples, "bookMemoryBytes"),
    }),
    [samples],
  );

  const last = samples[samples.length - 1];
  const statusClass = status === "live" ? "live" : status === "stalled" ? "stalled" : "offline";

  return (
    <main>
      <header className="top">
        <h1>nqbook monitor</h1>
        <span className={`pill ${statusClass}`}>
          <span className="dot" />
          {STATUS_TEXT[status]}
        </span>
        {endpoint && <span className="endpoint">{endpoint}</span>}
      </header>

      {samples.length < 2 ? (
        <div className="waiting">
          waiting for samples from the metrics stream
          {status === "offline" && " — is the dev server's ZMQ bridge running?"}
        </div>
      ) : (
        <>
          <h2 className="section">Throughput</h2>
          <div className="grid">
            <Tile label="Book events" points={series.bookEvents} format={fmtCount} unit="/s" wide smooth />
            <Tile label="Feed messages" points={series.feedMsg} format={fmtCount} unit="/s" smooth />
            <Tile label="Feed volume" points={series.feedBytes} format={fmtBytes} unit="/s" smooth />
            <Tile label="Writer rows" points={series.writerRows} format={fmtCount} unit="/s" smooth />
          </div>

          <h2 className="section">Book</h2>
          <div className="grid">
            <Tile label="Apply latency (sampled avg)" points={series.latency} format={fmtNs} wide height={90} />
            <Tile label="Resting orders" points={series.resting} format={fmtCount} />
            <Tile label="Instruments" points={series.instruments} format={fmtCount} />
            <Tile label="Book memory" points={series.memory} format={fmtBytes} />
          </div>

          <h2 className="section">Totals since start</h2>
          <div className="totals">
            <span className="item">
              <span className="k">feed messages</span>
              <span className="v">{last.feedMessages.toLocaleString()}</span>
            </span>
            <span className="item">
              <span className="k">orders</span>
              <span className="v">{last.feedOrders.toLocaleString()}</span>
            </span>
            <span className="item">
              <span className="k">trades</span>
              <span className="v">{last.feedTrades.toLocaleString()}</span>
            </span>
            <span className="item">
              <span className="k">book events</span>
              <span className="v">{last.bookEvents.toLocaleString()}</span>
            </span>
            <span className="item">
              <span className="k">writer rows</span>
              <span className="v">
                {(last.writerOrders + last.writerTrades + last.writerBooks).toLocaleString()}
              </span>
            </span>
            <span className="item">
              <span className="k">dropped frames</span>
              <span className={last.feedDropped > 0 ? "v bad" : "v"}>
                {last.feedDropped > 0 ? "⚠ " : ""}
                {last.feedDropped.toLocaleString()}
              </span>
            </span>
          </div>

          <details className="raw">
            <summary>latest raw sample</summary>
            <table>
              <tbody>
                {Object.entries(last).map(([key, value]) => (
                  <tr key={key}>
                    <th>{key}</th>
                    <td>{value.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        </>
      )}
    </main>
  );
}
