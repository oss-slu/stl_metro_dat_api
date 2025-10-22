"""
Excel Processor

Purpose
-------
Normalize raw Excel sheets with pandas and publish the cleaned dataset to Kafka.

Key behaviors
-------------
1) Standardize column names (lowercase, snake_case).
2) Optional column remapping (after normalization).
3) Enforce a required schema (create missing columns).
4) Treat empty strings as missing, drop fully-empty rows, then optionally fill NA.
5) Serialize to JSON (records) and send to Kafka.
6) Avoid keeping references to raw data; delete locals and run GC.

Input contract
--------------
process_and_send(raw_sheets: dict[str, pd.DataFrame])  # typically from excel_fetcher.fetch_excel()

Producer contract
-----------------
A minimal producer with:
- send(topic: str, value: bytes) -> None
- flush() -> None  # optional, called if present

Return value
------------
Dict summary with {success, topic, rows, cols, bytes_sent, [error]}

Notes on memory hygiene
-----------------------
We never store raw DataFrames on self. We work in local variables only.
Before returning we delete large locals and call gc.collect().
"""

from __future__ import annotations
from typing import Dict, Optional, List, Any
import gc
import pandas as pd


def _normalize_col(col: str) -> str:
    """
    Normalize a single column name to snake_case:
    - strip outer whitespace
    - lowercase
    - replace non-alphanumeric runs with underscores
    - collapse repeated underscores
    - strip leading/trailing underscores
    """
    import re
    s = str(col).strip().lower()
    s = re.sub(r"[^0-9a-z]+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s


class ExcelProcessor:
    """
    Parameters
    ----------
    producer : object
        Must implement send(topic, value) and optionally flush().
    topic : str
        Kafka topic to publish to. Default: "excel-processed-topic".
    column_map : dict[str,str] | None
        Mapping of normalized source column -> desired normalized column.
        Applied AFTER normalization. Unmapped columns are preserved.
    required_columns : list[str] | None
        Normalized column names to enforce across all sheets.
        Missing required columns are created with <NA>.
    fill_missing : bool
        If True, fill remaining NAs after dropping blank rows:
          - numeric -> 0
          - datetime -> leave as NA
          - object/string -> ""
    fail_on_produce_error : bool
        If True, re-raise producer exceptions. Else set success=False and attach error.
    """

    def __init__(
        self,
        producer: Any,
        topic: str = "excel-processed-topic",
        *,
        column_map: Optional[Dict[str, str]] = None,
        required_columns: Optional[List[str]] = None,
        fill_missing: bool = True,
        fail_on_produce_error: bool = False,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._column_map = column_map or {}
        self._required = required_columns or []
        self._fill_missing = fill_missing
        self._fail_on_error = fail_on_produce_error

    # ======================================================================
    # Public API
    # ======================================================================

    def process_and_send(self, raw_sheets: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Normalize input sheets and publish to Kafka.

        Returns
        -------
        dict: {
          "success": bool,
          "topic": str,
          "rows": int,
          "cols": int,
          "bytes_sent": int,
          ["error": str]
        }
        """
        # --- Normalize into a single DataFrame
        normalized_df = self._normalize_all_sheets(raw_sheets)

        # --- Serialize to JSON bytes (records)
        payload_bytes = self._serialize_df_to_json_bytes(normalized_df)

        # --- Attempt publish
        result = {
            "success": True,
            "topic": self._topic,
            "rows": int(normalized_df.shape[0]),
            "cols": int(normalized_df.shape[1]),
            "bytes_sent": len(payload_bytes),
        }

        try:
            self._producer.send(self._topic, payload_bytes)
            if hasattr(self._producer, "flush"):
                self._producer.flush()
        except Exception as e:
            result["success"] = False
            result["error"] = f"{type(e).__name__}: {e}"
            # Clean large locals before re-raise if configured
            if self._fail_on_error:
                del normalized_df
                del payload_bytes
                del raw_sheets
                gc.collect()
                raise

        # --- Memory hygiene: drop references and collect
        del normalized_df
        del payload_bytes
        del raw_sheets
        gc.collect()

        return result

    # ======================================================================
    # Normalization pipeline
    # ======================================================================

    def _normalize_all_sheets(self, raw_sheets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Per sheet:
          1) normalize column names
          2) apply column_map (if any)
          3) ensure required columns exist
          4) stable column order: required first, then others
          5) coerce empty strings -> <NA>, then drop fully-empty rows
          6) optionally fill remaining NA dtype-aware
          7) add 'sheet_name' column
        Finally concatenate sheets.
        """
        frames: List[pd.DataFrame] = []

        for sheet_name, df in raw_sheets.items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Sheet '{sheet_name}' is not a pandas DataFrame")

            # Work on a shallow copy to avoid mutating caller's data
            w = df.copy(deep=False)

            # 1) normalize column names
            w.columns = [_normalize_col(c) for c in w.columns]

            # 2) apply column map after normalization
            if self._column_map:
                w = w.rename(columns=self._column_map)

            # 3) ensure required columns exist
            for col in self._required:
                if col not in w.columns:
                    w[col] = pd.NA

            # 4) stable order: required first, then the rest
            if self._required:
                tail = [c for c in w.columns if c not in self._required]
                w = w[self._required + tail]

            # 5) coerce empty strings to NA, then drop all-NA rows
            w = self._coerce_empty_to_na(w)
            all_na_rows = w.isna().all(axis=1)
            w = w.loc[~all_na_rows].reset_index(drop=True)

            # 6) fill remaining NA dtype-aware (only if enabled)
            if self._fill_missing:
                w = self._fill_missing_dtype_aware(w)

            # 7) tag origin sheet
            w.insert(0, "sheet_name", sheet_name)

            frames.append(w)

        if not frames:
            # Return an empty DataFrame with minimal structure for downstream safety
            return pd.DataFrame(columns=["sheet_name"] + list(self._required))

        # Concatenate all sheets; no second all-NA drop required
        out = pd.concat(frames, ignore_index=True, sort=False)
        return out

    def _coerce_empty_to_na(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert blank-like strings to <NA> WITHOUT filling yet.
        Reason:
          We want to drop rows that are entirely empty after this coercion.
          Filling happens later so we do not accidentally preserve blank rows.
        """
        for c in df.columns:
            # Only object dtype can contain str values mixed with others
            if pd.api.types.is_object_dtype(df[c].dtype):
                df[c] = df[c].apply(
                    lambda x: pd.NA if (isinstance(x, str) and x.strip() == "") else x
                )
        return df

    def _fill_missing_dtype_aware(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill remaining NA values with conservative defaults:
          - numeric -> 0
          - datetime -> leave as NA
          - object/string -> ""
        """
        for c in df.columns:
            dt = df[c].dtype
            if pd.api.types.is_numeric_dtype(dt):
                df[c] = df[c].fillna(0)
            elif pd.api.types.is_datetime64_any_dtype(dt):
                # leave as NA to avoid fabricating dates
                pass
            else:
                df[c] = df[c].fillna("")
        return df

    # ======================================================================
    # Serialization
    # ======================================================================

    def _serialize_df_to_json_bytes(self, df: pd.DataFrame) -> bytes:
        """
        Serialize the cleaned DataFrame into UTF-8 JSON bytes.
        Format: list of record dicts. Dates use ISO format.
        Example:
            [{"sheet_name":"Sheet1","name":"Alice","age":23}, ...]
        """
        json_text = df.to_json(orient="records", date_format="iso", force_ascii=False)
        return json_text.encode("utf-8")
