# src/write_service/ingestion/fetcher.py
"""
Simple HTML fetcher & parser.

Public API:
    fetch_data(url: str) -> list[dict]

Behavior:
- Fetches an HTML page using requests (timeout=5).
- Parses the *first* <table> found in the page: header cells (<th>) become keys,
  rows become dicts.
- Tries to coerce numeric-looking strings into int/float for nicer JSON-like
  primitives.
- Raises RuntimeError for network/timeouts/HTTP errors.
- Raises ValueError if no table is found in the HTML.

Return:
    A list of dictionaries like: [{"ID": 1, "Name": "Central Station", "Ridership": 1200}, ...]
"""

from typing import Any
import requests
from bs4 import BeautifulSoup


def _coerce_type(value: str) -> Any:
    """
    Try to convert a string to int, then float; otherwise return original string.
    Keeps return values JSON-serializable (ints, floats, strings).
    """
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return ""
    # Try integer
    try:
        return int(value)
    except ValueError:
        pass
    # Try float
    try:
        return float(value)
    except ValueError:
        pass
    # Fallback to original string
    return value


def fetch_data(url: str) -> list[dict]:
    """
    Fetch a URL and parse the first HTML <table> into a list of dicts.

    Args:
        url: The HTTP/HTTPS URL to fetch.

    Returns:
        A list of dictionaries (one per table row). Keys are column headers
        (text from <th>) or "col_0", "col_1", ... if headers are missing.

    Raises:
        RuntimeError: on network/timeout/HTTP errors.
        ValueError: if no table is found in the HTML.
    """
    try:
        # Use a short timeout so caller doesn't hang; raise_for_status will raise
        # an HTTPError for 4xx/5xx responses.
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        # Wrap all requests-related exceptions in RuntimeError for the caller.
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc

    # Parse with BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find the first table in the page
    table = soup.find("table")
    if not table:
        # Explicit error for missing data (acceptance criterion).
        raise ValueError("No <table> element found in the HTML page.")

    # Determine headers: prefer <th>; if not present, fall back to first row's <td>.
    headers = []
    header_cells = table.find_all("th")
    if header_cells:
        headers = [th.get_text(strip=True) for th in header_cells]
        data_rows = table.find_all("tr")[1:]  # skip header row
    else:
        # If there are no ths, try to use the first tr's cells as headers
        all_rows = table.find_all("tr")
        if not all_rows:
            # empty table
            return []
        first_row_cells = all_rows[0].find_all(["td", "th"])
        headers = [c.get_text(strip=True) for c in first_row_cells]
        data_rows = all_rows[1:]  # subsequent rows are data

    results = []
    for row in data_rows:
        cols = row.find_all(["td", "th"])
        # Skip empty rows
        if not cols:
            continue
        values = [c.get_text(strip=True) for c in cols]

        # If the row has fewer columns than headers, pad with None
        if len(values) < len(headers):
            values.extend([None] * (len(headers) - len(values)))

        # Build a dict for the row
        if headers:
            row_dict = {}
            for i, val in enumerate(values):
                key = headers[i] if i < len(headers) and headers[i] else f"col_{i}"
                row_dict[key] = _coerce_type(val) if val is not None else None
        else:
            # No headers: use generic keys
            row_dict = {f"col_{i}": _coerce_type(val) for i, val in enumerate(values)}

        results.append(row_dict)

    return results