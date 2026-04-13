"""Database schema management — DDL for system and user tables."""

import sqlite3
from typing import Dict, List


def init_db(conn: sqlite3.Connection) -> None:
    """Create system tables (_sources, _table_metadata) if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS _sources (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path   TEXT NOT NULL,
            file_hash   TEXT NOT NULL,
            sheet_name  TEXT NOT NULL,
            table_name  TEXT NOT NULL,
            row_count   INTEGER,
            ingested_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS _table_metadata (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name      TEXT NOT NULL,
            column_name     TEXT NOT NULL,
            column_type     TEXT,
            original_name   TEXT,
            description     TEXT DEFAULT '',
            UNIQUE(table_name, column_name)
        );
    """)
    conn.commit()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info([{table_name}])").fetchall()
    return [r[1] if isinstance(r, tuple) else r["name"] for r in rows]


def create_user_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: Dict[str, str],
    drop_if_exists: bool = False,
) -> None:
    """Create a table with the given columns plus id and _source_id."""
    if drop_if_exists:
        conn.execute(f"DROP TABLE IF EXISTS [{table_name}]")

    col_defs = ",\n            ".join(
        f"[{name}] {sql_type}" for name, sql_type in columns.items()
    )
    sql = f"""
        CREATE TABLE IF NOT EXISTS [{table_name}] (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            {col_defs},
            _source_id  INTEGER REFERENCES _sources(id)
        )
    """
    conn.execute(sql)
    conn.commit()


def add_columns_if_missing(
    conn: sqlite3.Connection, table_name: str, new_columns: Dict[str, str]
) -> List[str]:
    """Add any columns in new_columns that don't already exist. Returns list of added column names."""
    existing = set(get_table_columns(conn, table_name))
    added = []
    for name, sql_type in new_columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{name}] {sql_type}")
            added.append(name)
    if added:
        conn.commit()
    return added


def upsert_table_metadata(
    conn: sqlite3.Connection,
    table_name: str,
    col_info: List[Dict],
) -> None:
    """Insert or update column metadata. Each dict needs: column_name, column_type, original_name."""
    for info in col_info:
        conn.execute(
            """
            INSERT INTO _table_metadata (table_name, column_name, column_type, original_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(table_name, column_name) DO UPDATE SET
                column_type = excluded.column_type,
                original_name = excluded.original_name
            """,
            (
                table_name,
                info["column_name"],
                info.get("column_type", "TEXT"),
                info.get("original_name", ""),
            ),
        )
    conn.commit()
