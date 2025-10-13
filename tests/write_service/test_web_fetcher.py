# tests/write_service/test_web_fetcher.py

import sys
import pathlib
import pytest

ROOT_SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))

from write_service.ingestion.web_fetcher import fetch_data


def _load_fixture_text(name: str) -> str:
    path = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / name
    return path.read_text(encoding="utf-8")


def test_fetch_data_parses_table(requests_mock):
    html = _load_fixture_text("sample_table.html")
    url = "https://example.com/mock-data"
    requests_mock.get(url, text=html, status_code=200)

    result = fetch_data(url)

    assert isinstance(result, list)
    assert len(result) == 2
    expected = [
        {"ID": 1, "Name": "Central Station", "Ridership": 1200},
        {"ID": 2, "Name": "North Ave", "Ridership": 830},
    ]
    assert result == expected


def test_fetch_data_http_error(requests_mock):
    url = "https://example.com/bad"
    requests_mock.get(url, status_code=500, text="Server Error")
    with pytest.raises(RuntimeError):
        fetch_data(url)


def test_fetch_data_timeout(monkeypatch):
    import requests
    def _raise_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout("simulated timeout")

    monkeypatch.setattr("requests.get", _raise_timeout)
    with pytest.raises(RuntimeError):
        fetch_data("https://example.com/timeout")
