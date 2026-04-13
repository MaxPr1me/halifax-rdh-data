"""Query helpers — run SQL and return DataFrames."""

import sqlite3
from typing import Optional

import pandas as pd

from murb_db.config import get_connection


def query(sql: str, params=None, conn: Optional[sqlite3.Connection] = None) -> pd.DataFrame:
    """Run a SQL query and return results as a DataFrame."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    finally:
        if own_conn:
            conn.close()


def list_tables(conn: Optional[sqlite3.Connection] = None) -> list:
    """List all user tables (excludes system tables starting with _)."""
    df = query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY name",
        conn=conn,
    )
    return df["name"].tolist()


def describe_table(table_name: str, conn: Optional[sqlite3.Connection] = None) -> pd.DataFrame:
    """Show column info for a table, joined with metadata descriptions."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        # Get pragma info
        cols_df = pd.read_sql(f"PRAGMA table_info([{table_name}])", conn)
        cols_df = cols_df.rename(columns={"name": "column_name", "type": "column_type"})
        cols_df = cols_df[["column_name", "column_type"]]

        # Get metadata
        meta_df = query(
            "SELECT column_name, original_name, description FROM _table_metadata WHERE table_name = ?",
            params=(table_name,),
            conn=conn,
        )
        if not meta_df.empty:
            cols_df = cols_df.merge(meta_df, on="column_name", how="left")

        return cols_df
    finally:
        if own_conn:
            conn.close()


def search_columns(pattern: str, conn: Optional[sqlite3.Connection] = None) -> pd.DataFrame:
    """Search for columns matching a LIKE pattern across all tables."""
    return query(
        "SELECT table_name, column_name, column_type, description FROM _table_metadata WHERE column_name LIKE ?",
        params=(f"%{pattern}%",),
        conn=conn,
    )


def get_schema_summary(conn: Optional[sqlite3.Connection] = None) -> str:
    """
    Produce a text summary of the entire database schema.
    Designed for LLM consumption — an AI can read this and write correct SQL.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        tables = list_tables(conn=conn)
        if not tables:
            return "Database is empty. No tables found."

        lines = ["DATABASE SCHEMA", "=" * 60, ""]
        for tbl in tables:
            # Row count
            count_df = query(f"SELECT COUNT(*) as cnt FROM [{tbl}]", conn=conn)
            row_count = count_df["cnt"].iloc[0]

            # Source info
            source_df = query(
                "SELECT DISTINCT file_path, sheet_name FROM _sources WHERE table_name = ?",
                params=(tbl,),
                conn=conn,
            )
            sources = "; ".join(
                f"{r['file_path']}:{r['sheet_name']}" for _, r in source_df.iterrows()
            ) if not source_df.empty else "unknown"

            lines.append(f"Table: {tbl} ({row_count} rows)")
            lines.append(f"  Source: {sources}")

            # Columns
            desc_df = describe_table(tbl, conn=conn)
            for _, row in desc_df.iterrows():
                col_name = row["column_name"]
                col_type = row.get("column_type", "")
                desc = row.get("description", "") if "description" in row else ""
                desc_str = f" — {desc}" if desc else ""
                lines.append(f"  {col_name:<30} {col_type:<10}{desc_str}")
            lines.append("")

        return "\n".join(lines)
    finally:
        if own_conn:
            conn.close()
