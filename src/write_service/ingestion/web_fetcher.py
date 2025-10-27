# src/write_service/ingestion/web_fetcher.py
"""
web_fetcher.py - HTML Table Fetcher for St. Louis Data Sources
Robust HTML table fetcher module with multi-table support.

Public API:
    fetch_data(url: str, table_selector: str | None = None) -> list[dict[str, Any]]
    fetch_all_tables(url: str) -> dict[str, list[dict[str, Any]]]

Responsibilities:
- Clean data (remove duplicates, strip whitespace)
- Validate data structure
- Serialize to JSON and publish to Kafka
- Manage memory (delete raw data after send)
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
    
    # Remove currency symbols
    s_no_currency = s.replace("$", "").replace("€", "").replace("£", "")
    s_no_commas = s_no_currency.replace(",", "")
    # Handle percentages
    if s_no_commas.endswith("%"):
        try:
            return float(s_no_commas.rstrip("%")) / 100.0
        except ValueError:
            return s
    # Try numeric conversion
    try:
        if _RE_NUMBER.match(s_no_commas):
            return float(s_no_commas) if "." in s_no_commas else int(s_no_commas)
    except Exception:
        pass
    return s


def _extract_table(soup: BeautifulSoup, selector: Optional[str]) -> Optional[BeautifulSoup]:
    """
    Find and return a <table> element from the page using optional CSS selector.
    
    Different websites structure tables differently. This function
    implements a fallback strategy: try custom selector first, then
    common patterns (id="data", class="data"), finally any table.
    """
    if selector:
        try:
            table = soup.select_one(selector)
            if table:
                return table
        except Exception as exc:
            logger.warning(f"Ignoring invalid selector '{selector}': {exc}")
    
    # Fallback strategy: try common table patterns
    for key in [("table", {"id": "data"}), ("table", {"class": "data"}), ("table", {})]:
        table = soup.find(*key)
        if table:
            return table
    return None


def _parse_table(table: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Parse a single HTML table into a list of dictionaries.

    Args:
        table: BeautifulSoup table element
        
    Returns:
        List of row dictionaries with headers as keys
    """
    # Identify headers
    headers = []
    data_rows = []
    thead = table.find("thead")
    
    if thead:
        # Standard table structure: <thead> with <th> elements
        headers = [_clean_text(th) for th in thead.find_all("th")]
        data_rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")[1:]
    else:
        # Alternative: first row contains headers
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
        
        # Pad with None if row has fewer columns than headers
        if len(values) < len(headers):
            values.extend([None] * (len(headers) - len(values)))
        
        # Create dictionary with proper headers or fallback column names
        row_dict = {
            headers[i] if i < len(headers) and headers[i] else f"col_{i}": _coerce_type(val)
            for i, val in enumerate(values)
        }
        results.append(row_dict)

    return results


def fetch_data(url: str, table_selector: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch the first table from a webpage and parse it into structured data.

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

    results = _parse_table(table)
    logger.info(f"Fetched {len(results)} rows successfully from {url}")
    return results


def fetch_all_tables(url: str, table_prefix: str = "table") -> Dict[str, List[Dict[str, Any]]]:
    # Fetch ALL tables from a webpage and parse them into structured data.
    logger.info(f"Fetching all tables from: {url}")
    
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.find_all("table")
    
    if not tables:
        raise ValueError(f"No tables found on page: {url}")
    
    results = {}
    for idx, table in enumerate(tables):
        # Try to find a caption or heading before the table
        # This makes the keys more meaningful
        caption = table.find("caption")
        if caption:
            key = _clean_text(caption).replace(" ", "_").lower()
        else:
            # Look for a heading immediately before the table
            prev_sibling = table.find_previous_sibling(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            if prev_sibling:
                key = _clean_text(prev_sibling).replace(" ", "_").lower()
            else:
                key = f"{table_prefix}_{idx}"
        
        parsed_data = _parse_table(table)
        if parsed_data:  # Only include non-empty tables
            results[key] = parsed_data
            logger.info(f"Parsed table '{key}' with {len(parsed_data)} rows")
    
    logger.info(f"Successfully fetched {len(results)} tables from {url}")
    return results


# Example usage documentation
if __name__ == "__main__":
    # Example 1: Single table (backward compatible)
    url_single = "https://example.com/single-table-page"
    try:
        data = fetch_data(url_single)
        print(f"Fetched {len(data)} rows")
    except Exception as e:
        print(f"Error: {e}")
    
    # Example 2: Multiple tables (new feature)
    url_multi = "https://www.stlouis-mo.gov/government/departments/planning/research/census/data/neighborhoods/neighborhood.cfm?number=35&censusYear=2020&comparisonYear=0"
    try:
        all_tables = fetch_all_tables(url_multi)
        for table_name, table_data in all_tables.items():
            print(f"\nTable: {table_name}")
            print(f"Rows: {len(table_data)}")
            if table_data:
                print(f"Sample row: {table_data[0]}")
    except Exception as e:
        print(f"Error: {e}")