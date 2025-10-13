# src/write_service/ingestion/web_fetcher.py
"""
Robust HTML table fetcher module.

Public API:
    fetch_data(url: str, table_selector: str | None = None) -> list[dict[str, Any]]

Responsibilities:
- Fetch HTML content from a given URL.
- Extract the first (or specified) table element.
- Parse rows into a list of dicts.
- Normalize and type-coerce cell values (numbers, percents, currency).
"""

from typing import Any, List, Optional, Dict
import re
import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# regex to detect numeric with optional decimal/negatives
_RE_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")


def _clean_text(elem) -> str:
    """Extract text from a BeautifulSoup element, normalizing whitespace."""
    return elem.get_text(" ", strip=True) if elem is not None else ""


def _coerce_type(value: Optional[str]) -> Any:
    """
    Convert textual numeric forms to Python types.
    Examples:
        "1,234" -> 1234
        "12.3" -> 12.3
        "45%" -> 0.45
        "$99" -> 99
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return ""
    s_no_currency = s.replace("$", "").replace("€", "").replace("£", "")
    s_no_commas = s_no_currency.replace(",", "")
    if s_no_commas.endswith("%"):
        try:
            return float(s_no_commas.rstrip("%")) / 100.0
        except ValueError:
            return s
    try:
        if _RE_NUMBER.match(s_no_commas):
            return float(s_no_commas) if "." in s_no_commas else int(s_no_commas)
    except Exception:
        pass
    return s


def _extract_table(soup: BeautifulSoup, selector: Optional[str]) -> Optional[BeautifulSoup]:
    """Find and return a <table> element from the page using optional CSS selector."""
    if selector:
        try:
            table = soup.select_one(selector)
            if table:
                return table
        except Exception as exc:
            logger.warning(f"Ignoring invalid selector '{selector}': {exc}")
    for key in [("table", {"id": "data"}), ("table", {"class": "data"}), ("table", {})]:
        table = soup.find(*key)
        if table:
            return table
    return None


def fetch_data(url: str, table_selector: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch a table from a webpage and parse it into structured data.

    Args:
        url: Target page URL.
        table_selector: Optional CSS selector (e.g., "#data" or "table.striped").

    Returns:
        List of dictionaries, one per row.

    Raises:
        RuntimeError: If the network request fails or times out.
        ValueError: If no valid table is found.
    """
    logger.info(f"Fetching data from: {url}")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc

    soup = BeautifulSoup(resp.text, "html.parser")
    table = _extract_table(soup, table_selector)
    if not table:
        raise ValueError(f"No table found for selector: {table_selector}")

    # Identify headers
    headers = []
    data_rows = []
    thead = table.find("thead")
    if thead:
        headers = [_clean_text(th) for th in thead.find_all("th")]
        data_rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")[1:]
    else:
        first_row = table.find("tr")
        if not first_row:
            return []
        maybe_ths = first_row.find_all("th")
        if maybe_ths:
            headers = [_clean_text(th) for th in maybe_ths]
            data_rows = table.find_all("tr")[1:]
        else:
            headers = [_clean_text(td) for td in first_row.find_all(["td", "th"])]
            data_rows = table.find_all("tr")[1:]

    results = []
    for row in data_rows:
        cols = row.find_all(["td", "th"])
        if not cols:
            continue
        values = [_clean_text(c) for c in cols]
        if len(values) < len(headers):
            values.extend([None] * (len(headers) - len(values)))
        row_dict = {
            headers[i] if i < len(headers) and headers[i] else f"col_{i}": _coerce_type(val)
            for i, val in enumerate(values)
        }
        results.append(row_dict)

    logger.info(f"Fetched {len(results)} rows successfully from {url}")
    return results
