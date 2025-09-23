"""
events.py

Stub event processor for the read-side (Kafka mock consumer).

Why this exists:
- In a full system, the read-service would consume Kafka events that were
  published by the write-service (e.g., new data being ingested).
- For now, we don’t have that integration yet, so this file simulates
  a Kafka consumer by logging fake "events" to prove the structure works.

How it works:
- start_mock_consumer() launches a background thread that runs _mock_loop().
- _mock_loop() just sleeps a bit, then prints fake "processed events."
- The Flask app imports and starts this stub when it boots.
"""

import os
import threading
import time


def _mock_loop(logger, broker: str):
    """
    Private helper that simulates a Kafka consumer loop.
    Parameters:
        logger: Flask app’s logger, used to print info messages.
        broker: the Kafka broker string (comes from env var or default).
    Behavior:
        - Pretends to "connect" to the broker.
        - Iterates 3 times, each time waiting 1 second, logging a fake event.
        - Logs when it stops.
    """
    logger.info(f"[read_service] mock consumer connected to broker={broker}")
    for i in range(3):
        time.sleep(1)  # Simulate waiting for an event
        logger.info(f"[read_service] processed mock event #{i}")
    logger.info("[read_service] mock consumer stopped")


def start_mock_consumer(logger) -> threading.Thread:
    """
    Public function that starts the stub Kafka consumer.

    Steps:
    - Reads KAFKA_BROKER from environment variables (default: localhost:9092).
    - Creates a background thread (daemon=True so it won’t block shutdown).
    - Runs _mock_loop() inside that thread.
    - Returns the thread object (in case caller wants to track it).

    Usage:
        from read_service.processors.events import start_mock_consumer
        start_mock_consumer(app.logger)
    """
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    t = threading.Thread(target=_mock_loop, args=(logger, broker), daemon=True)
    t.start()
    return t
