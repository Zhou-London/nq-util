# /// script
# requires-python = ">=3.12"
# dependencies = ["pyzmq>=26"]
# ///
"""Feed simulator: publishes one repeated virtual order to nqbook over ZMQ.

Each message is framed as nqbook's RunFeed expects: one tag byte (0 = order,
1 = trade), then the record in nlib::order host layout. Only `seq` and
`event_ns` change between sends.
"""

import argparse
import struct
import time

import zmq

ORDER_TAG = 0

# nlib::order, little-endian LP64: seq, order_id, price, qty, cancel_qty,
# new_qty, event_ns (int64), prev, next (uint64, zeroed; ignored on receive),
# instrument_id (uint32), side, type, action (uint8), one pad byte, recv_ns
# (int64, zeroed; stamped by the receiver).
ORDER_FORMAT = struct.Struct("<qqqqqqqQQIBBBxq")
assert ORDER_FORMAT.size == 88

ORDER_ID = 1
PRICE = 1_001_000_000_000  # 100.10 at nlib::price_scale (1/1e10)
QTY = 1_000_000_000  # 10 at nlib::qty_scale (1/1e8)
INSTRUMENT_ID = 1
SIDE_BUY = 0
TYPE_LIMIT = 0
ACTION_ADD = 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--endpoint", default="tcp://*:5555", help="PUB bind endpoint")
    parser.add_argument(
        "--interval", type=float, default=0.1, help="seconds between messages"
    )
    args = parser.parse_args()

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind(args.endpoint)
    print(f"feed_sim: publishing on {args.endpoint} every {args.interval}s")

    seq = 0
    try:
        while True:
            seq += 1
            payload = ORDER_FORMAT.pack(
                seq,
                ORDER_ID,
                PRICE,
                QTY,
                0,  # cancel_qty: an add takes nothing out of the book
                0,  # new_qty: an add sets no new remaining quantity
                time.time_ns(),
                0,
                0,
                INSTRUMENT_ID,
                SIDE_BUY,
                TYPE_LIMIT,
                ACTION_ADD,
                0,
            )
            pub.send(bytes([ORDER_TAG]) + payload)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        pub.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()
