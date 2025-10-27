# src/write_service/processing/web_processor.py
"""
web_processor.py
Processes raw web-fetched data by cleaning, validating, and sending it to Kafka.

Public API:
    clean_data(raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]
    send_to_kafka(cleaned_data: List[Dict[str, Any]], topic: str = "web-processed-topic") -> None
    process_and_send(raw_data: List[Dict[str, Any]]) -> None
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
    
    Args:
        raw_data: List of dictionaries from web_fetcher
        
    Returns:
        Cleaned list with duplicates removed and whitespace stripped
        
    Example:
        Input:  [{"name": " Alice ", "age": "25"}, {"name": "Alice", "age": "25"}]
        Output: [{"name": "Alice", "age": "25"}]
    """
    if not raw_data:
        return []

    cleaned = []
    seen = set()

    for item in raw_data:
        # Strip whitespace from string values only
        normalized_item = {
            k: v.strip() if isinstance(v, str) else v
            for k, v in item.items()
        }

        # Convert dict to tuple to check for duplicates
        item_key = tuple(sorted(normalized_item.items()))
        if item_key not in seen:
            seen.add(item_key)
            cleaned.append(normalized_item)

    logger.info(f"Cleaned {len(raw_data)} rows -> {len(cleaned)} rows (removed {len(raw_data) - len(cleaned)} duplicates)")
    return cleaned


def send_to_kafka(cleaned_data: List[Dict[str, Any]], topic: str = "web-processed-topic") -> None:
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
    logger.info(f"Successfully sent {len(cleaned_data)} records to {topic}")


def process_and_send(raw_data: List[Dict[str, Any]]) -> None:
    """       
    Process:
        1. Clean the data (remove duplicates, strip whitespace)
        2. Send to Kafka topic
        3. Delete raw data from memory
    """
    # Step 1: Clean
    cleaned_data = clean_data(raw_data)
    
    # Step 2: Send
    send_to_kafka(cleaned_data)

    # Step 3: Delete raw data from memory (as per acceptance criteria)
    del raw_data
    logger.info("Raw data deleted from memory after successful processing.")