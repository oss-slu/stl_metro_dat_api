"""
Unit tests for the Excel parser using pytest.
Covers:
- Reading a valid local Excel file.
- Handling of invalid file paths.
"""

import pytest
from write_service.ingestion.excel_parser import read_excel


def test_read_local_file(tmp_path):
    """
    Test case: successfully read a small local Excel file.

    Steps:
    1. Create a temporary Excel file with openpyxl.
    2. Add a header row ["Name", "Age"].
    3. Add two rows of data: ["Alice", 30], ["Bob", 25].
    4. Save file to a temporary path provided by pytest.
    5. Call read_excel() on that file.
    6. Assert that:
        - The sheet is present in the result.
        - The first row dict has Name = "Alice".
        - The second row dict has Age = 25.
    """

    from openpyxl import Workbook

    # 1. Create new workbook (default has one sheet named "Sheet")
    wb = Workbook()
    ws = wb.active

    # 2. Rename the sheet to "Sheet1" (common default)
    ws.title = "Sheet1"

    # 3. Write headers and rows
    ws.append(["Name", "Age"])   # header row
    ws.append(["Alice", 30])     # data row 1
    ws.append(["Bob", 25])       # data row 2

    # 4. Save workbook to a temporary file (pytest gives tmp_path)
    file_path = tmp_path / "test.xlsx"
    wb.save(file_path)

    # 5. Run the parser on that file
    result = read_excel(str(file_path))

    # 6. Assertions
    assert "Sheet1" in result                # Confirm sheet is present
    assert result["Sheet1"][0]["Name"] == "Alice"  # First row, "Name" = Alice
    assert result["Sheet1"][1]["Age"] == 25        # Second row, "Age" = 25


def test_bad_file():
    """
    Test case: try reading a nonexistent file.
    - Expect read_excel() to raise ValueError.
    """

    # Call with a fake filename → should raise ValueError
    with pytest.raises(ValueError):
        read_excel("nonexistent.xlsx")