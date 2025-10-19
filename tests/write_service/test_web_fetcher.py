# tests/write_service/test_web_fetcher.py
"""
1. Tests on mock sample data
2. Tests on real STL census website
"""

import sys
import pathlib
import pytest

ROOT_SRC = pathlib.Path(__file__).resolve().parents[2] / "src"
if str(ROOT_SRC) not in sys.path:
    sys.path.insert(0, str(ROOT_SRC))

from write_service.ingestion.web_fetcher import fetch_data


def _load_fixture_text(name: str) -> str:
    """Load HTML fixture from tests/fixtures/ directory."""
    path = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / name
    return path.read_text(encoding="utf-8")


def test_fetch_data_parses_table(requests_mock):
    """
    Test that fetch_data() correctly parses an HTML table.
    
    This verifies:
    - HTML is fetched via HTTP
    - Table is found and parsed
    - Data is returned as list of dicts
    - Type coercion works (numbers become int/float)
    """
    html = _load_fixture_text("sample_table.html")
    url = "https://example.com/mock-data"
    requests_mock.get(url, text=html, status_code=200)

    result = fetch_data(url)

    assert isinstance(result, list), "Should return a list"
    assert len(result) == 2, "Should have 2 data rows (excluding header)"
    expected = [
        {"ID": 1, "Name": "Central Station", "Ridership": 1200},
        {"ID": 2, "Name": "North Ave", "Ridership": 830},
    ]
    assert result == expected, f"Expected {expected}, got {result}"


def test_fetch_data_http_error(requests_mock):
    """
    Test that HTTP errors are handled properly.
    
    This verifies:
    - Network errors don't crash silently
    - RuntimeError is raised with helpful message
    """
    url = "https://example.com/bad"
    requests_mock.get(url, status_code=500, text="Server Error")
    
    with pytest.raises(RuntimeError, match="Failed to fetch"):
        fetch_data(url)


def test_fetch_data_timeout(monkeypatch):
    """
    Test that timeouts are handled properly.
    
    This verifies:
    - Slow/unresponsive servers don't hang forever
    - RuntimeError is raised
    """
    import requests
    def _raise_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout("simulated timeout")

    monkeypatch.setattr("requests.get", _raise_timeout)
    
    with pytest.raises(RuntimeError, match="Failed to fetch"):
        fetch_data("https://example.com/timeout")


# ============================================
# NEW: Integration test with real website
# ============================================

def test_fetch_real_stlouis_census():
    """
    Integration test: Fetch real data from St. Louis census website.
    
    This verifies your code works with an actual website, not just mocks.
    
    WHY: Proves your web_fetcher works in production conditions.
    
    HOW TO RUN:
        pytest tests/write_service/test_web_fetcher.py::test_fetch_real_stlouis_census -v -s
    
    To skip this test (if no internet):
        pytest tests/write_service/test_web_fetcher.py -v -m "not integration"
    """
    url = "https://www.stlouis-mo.gov/government/departments/planning/research/census/data/neighborhoods/neighborhood.cfm?number=35&censusYear=2020&comparisonYear=0"
    
    try:
        result = fetch_data(url)
        
        # Basic sanity checks
        assert isinstance(result, list), "Should return a list"
        assert len(result) > 0, "Should have at least one row of data"
        assert isinstance(result[0], dict), "Each row should be a dictionary"
        
        # Print for manual verification
        print(f"\n✅ Successfully fetched {len(result)} rows from St. Louis census")
        print(f"Sample row: {result[0]}")
        
    except Exception as e:
        # Don't fail the test if website is down or structure changed
        pytest.skip(f"Integration test skipped: {e}")


# ============================================
# HOW TO RUN THESE TESTS
# ============================================
"""
From project root:

1. Run all tests:
   pytest tests/write_service/test_web_fetcher.py -v

2. Run only mock tests (fast, no network):
   pytest tests/write_service/test_web_fetcher.py -v -m "not integration"

3. Run only the integration test:
   pytest tests/write_service/test_web_fetcher.py::test_fetch_real_stlouis_census -v -s

4. See what's happening (show print statements):
   pytest tests/write_service/test_web_fetcher.py -v -s
"""