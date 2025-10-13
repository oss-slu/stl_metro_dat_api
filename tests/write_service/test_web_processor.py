# tests/write_service/test_web_processor.py

import pytest
from unittest.mock import patch, MagicMock
from src.write_service.processing.web_processor import clean_data, process_and_send

def test_clean_data_removes_duplicates_and_whitespace():
    raw = [
        {"name": " Alice ", "age": "25"},
        {"name": "Alice", "age": "25"},
        {"name": "Bob ", "age": "30"}
    ]
    cleaned = clean_data(raw)
    assert len(cleaned) == 2
    assert cleaned[0]["name"] == "Alice"
    assert cleaned[1]["name"] == "Bob"


@patch("src.write_service.processing.web_processor.KafkaProducer")
def test_process_and_send_sends_to_kafka(MockProducer):
    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer

    raw_data = [{"city": "St. Louis ", "population": "300000"}]
    process_and_send(raw_data)

    # Kafka should have been called
    assert mock_producer.send.called
    mock_producer.close.assert_called_once()
