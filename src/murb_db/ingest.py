"""Excel ingestion pipeline — read, clean, type-detect, load to SQLite."""

import hashlib
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from murb_db.schema import (
    add_columns_if_missing,
    create_user_table,
    init_db,
    table_exists,
    upsert_table_metadata,
)


def clean_column_name(name: str) -> str:
    """Normalize a column name to snake_case."""
    s = str(name).strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)  # replace non-alnum with _
    s = re.sub(r"_+", "_", s)            # collapse consecutive _
    s = s.strip("_")
    return s or "unnamed"


def deduplicate_column_names(names: List[str]) -> List[str]:
    """Append _2, _3, etc. to resolve duplicate column names."""
    seen: Dict[str, int] = {}
    result = []
    for name in names:
        if name in seen:
            seen[name] += 1
            result.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 1
            result.append(name)
    return result


def detect_column_types(df: pd.DataFrame) -> Dict[str, str]:
    """Detect SQLite types for each column: INTEGER, REAL, or TEXT."""
    type_map = {}
    for col in df.columns:
        series = df[col].dropna()
        if series.empty:
            type_map[col] = "TEXT"
            continue
        # Try numeric
        numeric = pd.to_numeric(series, errors="coerce")
        if numeric.notna().sum() > 0.5 * len(series):
            # Check if integer-like
            if (numeric.dropna() == numeric.dropna().astype(int)).all():
                type_map[col] = "INTEGER"
            else:
                type_map[col] = "REAL"
            continue
        # Try datetime
        try:
            pd.to_datetime(series, errors="raise", infer_datetime_format=True)
            type_map[col] = "TEXT"  # store dates as ISO text in SQLite
            continue
        except (ValueError, TypeError):
            pass
        type_map[col] = "TEXT"
    return type_map


def cast_columns(df: pd.DataFrame, type_map: Dict[str, str]) -> pd.DataFrame:
    """Cast DataFrame columns according to the detected type map."""
    df = df.copy()
    for col, sql_type in type_map.items():
        if col not in df.columns:
            continue
        if sql_type in ("INTEGER", "REAL"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif sql_type == "TEXT":
            df[col] = df[col].astype(str).replace("nan", None)
    return df


def compute_file_hash(file_path: Path) -> str:
    """SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_file(
    file_path: Path,
    conn: sqlite3.Connection,
    registry: "Registry",
    force: bool = False,
) -> List[str]:
    """
    Ingest all sheets from an Excel file into SQLite.
    Returns list of table names created/updated.
    """
    from murb_db.registry import Registry

    file_path = Path(file_path)
    file_hash = compute_file_hash(file_path)

    # Check for duplicate
    if not force:
        existing = conn.execute(
            "SELECT id FROM _sources WHERE file_hash = ?", (file_hash,)
        ).fetchone()
        if existing:
            print(f"  Skipped (already ingested): {file_path.name}")
            return []

    init_db(conn)  # ensure system tables exist

    xls = pd.ExcelFile(file_path)
    tables_touched = []

    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name)
        if df.empty:
            continue

        # Clean column names
        original_names = list(df.columns)
        clean_names = deduplicate_column_names(
            [clean_column_name(c) for c in original_names]
        )
        df.columns = clean_names

        # Apply column renames from registry
        col_map = registry.resolve_column_map(file_path.name, sheet_name)
        if col_map:
            df = df.rename(columns=col_map)

        # Detect and cast types
        type_map = detect_column_types(df)
        df = cast_columns(df, type_map)

        # Resolve target table name
        target_table = registry.resolve_table_name(file_path.name, sheet_name)

        # Create or extend table
        if table_exists(conn, target_table):
            added = add_columns_if_missing(conn, target_table, type_map)
            if added:
                print(f"  Extended table '{target_table}' with columns: {added}")
        else:
            create_user_table(conn, target_table, type_map)
            print(f"  Created table '{target_table}' ({len(df)} rows)")

        # Insert source record
        cursor = conn.execute(
            """
            INSERT INTO _sources (file_path, file_hash, sheet_name, table_name, row_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(file_path), file_hash, sheet_name, target_table, len(df)),
        )
        source_id = cursor.lastrowid

        # Add _source_id and write to DB
        df["_source_id"] = source_id
        df.to_sql(target_table, conn, if_exists="append", index=False)

        # Update metadata
        col_info = [
            {
                "column_name": clean,
                "column_type": type_map.get(clean, "TEXT"),
                "original_name": orig,
            }
            for orig, clean in zip(original_names, clean_names)
        ]
        upsert_table_metadata(conn, target_table, col_info)
        tables_touched.append(target_table)

    conn.commit()
    return tables_touched


def ingest_directory(
    dir_path: Path,
    conn: sqlite3.Connection,
    registry: "Registry",
    pattern: str = "*.xlsx",
    force: bool = False,
) -> List[str]:
    """Ingest all matching Excel files from a directory."""
    dir_path = Path(dir_path)
    all_tables = []
    files = sorted(dir_path.glob(pattern))
    if not files:
        print(f"  No files matching '{pattern}' in {dir_path}")
        return []
    for fp in files:
        print(f"Ingesting: {fp.name}")
        tables = ingest_file(fp, conn, registry, force=force)
        all_tables.extend(tables)
    return all_tables
