from __future__ import annotations

import argparse
import json
import os
import uuid

from streaming.db import connect, ensure_streaming_tables


BOOKING_TOPIC = os.getenv("KAFKA_BOOKING_TOPIC", "booking_events")
PAYMENT_TOPIC = os.getenv("KAFKA_PAYMENT_TOPIC", "payment_events")


def count_rows(cur, table: str) -> int:
    cur.execute(f"select count(*) from {table}")
    return int(cur.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser(description="Write streaming reconciliation rows.")
    parser.add_argument("--run-id", default=os.getenv("STREAM_RUN_ID", "manual-stream-demo"))
    parser.add_argument("--booking-produced", type=int, required=True)
    parser.add_argument("--payment-produced", type=int, required=True)
    parser.add_argument("--duplicate-events", type=int, default=0)
    args = parser.parse_args()

    ensure_streaming_tables()
    with connect() as conn:
        with conn.cursor() as cur:
            booking_consumed = count_rows(cur, "raw.raw_booking_events_stream")
            payment_consumed = count_rows(cur, "raw.raw_payment_events_stream")
            cur.execute("select count(*) from metadata.streaming_dlq")
            dlq_total = int(cur.fetchone()[0])
            rows = [
                (BOOKING_TOPIC, args.booking_produced, booking_consumed),
                (PAYMENT_TOPIC, args.payment_produced, payment_consumed),
            ]
            output = []
            for topic, produced, consumed in rows:
                difference = produced - consumed
                status = "PASSED" if difference <= dlq_total + args.duplicate_events else "FAILED"
                cur.execute(
                    """
                    insert into metadata.streaming_reconciliation (
                        reconciliation_id,
                        run_id,
                        topic_name,
                        produced_events,
                        consumed_events,
                        duplicate_events,
                        dlq_events,
                        status,
                        difference
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(uuid.uuid4()),
                        args.run_id,
                        topic,
                        produced,
                        consumed,
                        args.duplicate_events,
                        dlq_total,
                        status,
                        difference,
                    ),
                )
                output.append({"topic": topic, "produced": produced, "consumed": consumed, "status": status})
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
