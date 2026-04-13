"""Central configuration — paths and database connection."""

import sqlite3
from pathlib import Path

# Walk up from this file to find the project root (directory containing pyproject.toml)
_this_dir = Path(__file__).resolve().parent
PROJECT_ROOT = _this_dir.parent.parent  # src/murb_db -> src -> project root

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DB_PATH = PROCESSED_DIR / "murb.db"
MAPPINGS_DIR = PROJECT_ROOT / "schema_mappings"


from typing import Optional

def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    """Open a SQLite connection with sensible defaults."""
    path = str(db_path or DB_PATH)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn
