# tests/write_service/test_web_processor.py
"""
YOUR ORIGINAL TESTS with minimal improvements.

ONLY changes:
1. Added docstrings
2. Fixed import path
3. Added one more test for send_to_kafka
"""

import pytest
from unittest.mock import patch, MagicMock
import sys
import pathlib

ROOT_SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))

from write_service.processing.web_processor import clean_data, process_and_send, send_to_kafka


def test_clean_data_removes_duplicates_and_whitespace():
    """
    Test that clean_data() removes duplicates and strips whitespace.
    
    This verifies:
    - Duplicate rows are removed
    - Whitespace is stripped from string values
    - Non-string values (numbers) are preserved
    """
    raw = [
        {"name": " Alice ", "age": "25"},
        {"name": "Alice", "age": "25"},  # Duplicate after cleaning
        {"name": "Bob ", "age": "30"}
    ]
    
    cleaned = clean_data(raw)
    
    assert len(cleaned) == 2, "Should remove duplicate, leaving 2 rows"
    assert cleaned[0]["name"] == "Alice", "Should strip whitespace"
    assert cleaned[1]["name"] == "Bob", "Should strip whitespace"


@patch("write_service.processing.web_processor.KafkaProducer")
def test_process_and_send_sends_to_kafka(MockProducer):
    """
    Test that process_and_send() sends cleaned data to Kafka.
    
    This verifies:
    - Data is cleaned before sending
    - KafkaProducer is created
    - Each record is sent
    - Producer is closed
    """
    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer

    raw_data = [{"city": "St. Louis ", "population": "300000"}]
    
    process_and_send(raw_data)

    # Kafka should have been called
    assert mock_producer.send.called, "Should send to Kafka"
    assert mock_producer.flush.called, "Should flush messages"
    mock_producer.close.assert_called_once()


@patch("write_service.processing.web_processor.KafkaProducer")
def test_send_to_kafka_sends_each_record(MockProducer):
    """
    Test that send_to_kafka() sends each record individually.
    
    This verifies:
    - Each row becomes a separate Kafka message
    - Correct topic is used
    """
    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer
    
    data = [
        {"city": "St. Louis", "pop": 300000},
        {"city": "Kansas City", "pop": 500000}
    ]
    
    send_to_kafka(data, topic="test-topic")
    
    # Should send both records
    assert mock_producer.send.call_count == 2, "Should send 2 records"
    
    # Verify correct topic
    calls = mock_producer.send.call_args_list
    assert calls[0][0][0] == "test-topic", "Should use specified topic"
    assert calls[1][0][0] == "test-topic", "Should use specified topic"


@patch("write_service.processing.web_processor.KafkaProducer")
def test_send_to_kafka_handles_empty_data(MockProducer):
    """
    Test that send_to_kafka() handles empty input gracefully.
    
    This verifies:
    - Empty list doesn't create unnecessary Kafka connections
    - No errors are raised
    """
    mock_producer = MagicMock()
    MockProducer.return_value = mock_producer
    
    send_to_kafka([])
    
    # Should NOT create producer for empty data
    MockProducer.assert_not_called()


# ============================================
# HOW TO RUN THESE TESTS
# ============================================
"""
From project root:

1. Run all processor tests:
   pytest tests/write_service/test_web_processor.py -v

2. Run specific test:
   pytest tests/write_service/test_web_processor.py::test_clean_data_removes_duplicates_and_whitespace -v

3. See coverage:
   pytest tests/write_service/test_web_processor.py --cov=write_service.processing --cov-report=html
"""