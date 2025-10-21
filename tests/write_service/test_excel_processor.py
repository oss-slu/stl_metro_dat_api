"""
Tests for ExcelProcessor.

Strategy
--------
- Use a simple in-memory MockProducer to avoid a real Kafka broker.
- Provide small sample sheets with messy columns and blanks.
- Verify:
  * normalization and concat behavior
  * JSON serialization shape
  * error-path behavior
  * no lingering "raw" references on the processor object
"""

import gc
import json
import weakref
import pandas as pd
import pytest

from write_service.processing.excel_processor import ExcelProcessor


class MockProducer:
    """In-memory collector. Optional failure injection via raise_on_send."""

    def __init__(self, raise_on_send: bool = False):
        self.raise_on_send = raise_on_send
        self.messages = []  # list[(topic, bytes)]

    def send(self, topic: str, value: bytes):
        if self.raise_on_send:
            raise RuntimeError("Simulated send failure")
        assert isinstance(topic, str)
        assert isinstance(value, (bytes, bytearray))
        self.messages.append((topic, bytes(value)))

    def flush(self):
        return


def make_raw():
    """Simulated excel_fetcher output: two sheets with messy headers and blanks."""
    s1 = pd.DataFrame(
        {
            "  Name ": ["Alice", "Bob", " "],  # third row is effectively blank
            "Age ": [23, None, None],
            " e-mail": ["a@x.com", "", None],
        }
    )
    s2 = pd.DataFrame(
        {
            "NAME": ["Cara", "Dan"],
            "AGE": [31, 28],
            "E Mail": ["c@x.com", "d@x.com"],
        }
    )
    return {"Sheet One": s1, "Other": s2}


def test_normalization_and_concat():
    prod = MockProducer()
    proc = ExcelProcessor(
        prod,
        column_map={"e_mail": "email"},  # "e-mail" -> normalized "e_mail" -> "email"
        required_columns=["name", "age", "email"],
    )

    raw = make_raw()
    df = proc._normalize_all_sheets(raw)

    # Required columns first, with sheet_name tagged
    assert list(df.columns)[:4] == ["sheet_name", "name", "age", "email"]

    # Blank row is removed after empty->NA coercion and all-NA drop
    assert len(df) == 4

    # Bob's missing email is filled with "" because fill_missing=True by default
    row_bob = df[df["name"] == "Bob"].iloc[0]
    assert row_bob["email"] == ""


def test_process_and_send_success_serializes_records():
    prod = MockProducer()
    proc = ExcelProcessor(prod, required_columns=["name", "age", "email"])
    result = proc.process_and_send(make_raw())

    assert result["success"] is True
    assert result["topic"] == "excel-processed-topic"
    assert result["rows"] == 4
    assert result["cols"] >= 4
    assert result["bytes_sent"] > 0

    # Producer got one message
    assert len(prod.messages) == 1
    topic, payload = prod.messages[0]
    assert topic == "excel-processed-topic"

    data = json.loads(payload.decode("utf-8"))
    assert isinstance(data, list) and len(data) == 4 and all(isinstance(r, dict) for r in data)


def test_process_and_send_handles_producer_error_without_raise():
    prod = MockProducer(raise_on_send=True)
    proc = ExcelProcessor(prod, fail_on_produce_error=False)
    result = proc.process_and_send(make_raw())

    assert result["success"] is False
    assert "error" in result
    assert len(prod.messages) == 0


def test_process_and_send_raises_when_configured():
    prod = MockProducer(raise_on_send=True)
    proc = ExcelProcessor(prod, fail_on_produce_error=True)
    with pytest.raises(RuntimeError):
        proc.process_and_send(make_raw())


def test_processor_does_not_hold_raw_refs():
    """
    Acceptance: Delete raw from memory after send.
    We verify by avoiding any 'raw' attributes on the processor and forcing GC.
    """
    prod = MockProducer()
    proc = ExcelProcessor(prod)

    big = pd.DataFrame({"x": range(10000)})
    ref = weakref.ref(big)

    def scope():
        proc.process_and_send({"Big": big})

    scope()
    del big
    gc.collect()

    # Heuristic: the processor itself should not store any attribute containing 'raw'
    assert not any(k for k in proc.__dict__.keys() if "raw" in k.lower())
