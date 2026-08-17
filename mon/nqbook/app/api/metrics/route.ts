// Bridges nqbook's ZMQ metrics stream to the browser: one conflated SUB
// socket per SSE client, each nlib::metrics frame forwarded as one event the
// moment it arrives. Conflation means a client that lags never reads a
// backlog — every delivery is the newest sample.

import type { NextRequest } from "next/server";
import { Subscriber } from "zeromq";

import { METRICS_SIZE, decode } from "@/lib/metrics";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const endpoint = process.env.NQBOOK_METRICS_ENDPOINT ?? "tcp://127.0.0.1:5556";
  const sub = new Subscriber({ conflate: true });
  sub.connect(endpoint);
  sub.subscribe();

  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let closed = false;
      const close = () => {
        if (closed) return;
        closed = true;
        sub.close();
        try {
          controller.close();
        } catch {}
      };
      request.signal.addEventListener("abort", close);

      controller.enqueue(encoder.encode(`event: hello\ndata: ${JSON.stringify({ endpoint })}\n\n`));
      try {
        for await (const [frame] of sub) {
          if (frame.length !== METRICS_SIZE) continue;
          controller.enqueue(encoder.encode(`data: ${JSON.stringify(decode(frame))}\n\n`));
        }
      } catch {
        // The socket was closed under the iterator; nothing to report.
      }
      close();
    },
    cancel() {
      sub.close();
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}
