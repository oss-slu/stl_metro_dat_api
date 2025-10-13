# src/write_service/processing/web_processor.py

"""
web_processor.py
Processes raw web-fetched data by cleaning, validating, and sending it to Kafka.
"""

from typing import List, Dict, Any
from kafka import KafkaProducer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Removes duplicates, strips whitespace, and normalizes text fields.
    """
    if not raw_data:
        return []

    cleaned = []
    seen = set()

    for item in raw_data:
        normalized_item = {
            k: v.strip() if isinstance(v, str) else v
            for k, v in item.items()
        }

        # Convert dict to tuple to check for duplicates
        item_key = tuple(sorted(normalized_item.items()))
        if item_key not in seen:
            seen.add(item_key)
            cleaned.append(normalized_item)

    return cleaned


def send_to_kafka(cleaned_data: List[Dict[str, Any]], topic: str = "web-processed-topic") -> None:
    """
    Sends cleaned data to a Kafka topic.
    """
    if not cleaned_data:
        logger.warning("No data to send to Kafka.")
        return

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for record in cleaned_data:
        producer.send(topic, record)
        logger.info(f"Sent record to {topic}: {record}")

    producer.flush()
    producer.close()


def process_and_send(raw_data: List[Dict[str, Any]]) -> None:
    """
    High-level function:
    - Cleans raw data
    - Sends to Kafka
    - Deletes raw data from memory after successful send
    """
    cleaned_data = clean_data(raw_data)
    send_to_kafka(cleaned_data)

    # Ensure deletion of raw data
    del raw_data
    logger.info("Raw data deleted from memory after successful processing.")
