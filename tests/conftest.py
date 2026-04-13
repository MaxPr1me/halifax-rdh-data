"""Shared test fixtures."""

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from murb_db.schema import init_db
from murb_db.registry import Registry


@pytest.fixture
def tmp_db():
    """In-memory SQLite database with system tables initialized."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_df():
    """A small DataFrame with mixed types for testing."""
    return pd.DataFrame({
        "Building Name": ["Tower A", "Tower B", "Tower C"],
        "Year Built": [1990, 2005, 2018],
        "Total Energy (kWh)": [150000.5, 220000.0, 95000.75],
        "MURB Type": ["high-rise", "mid-rise", "low-rise"],
    })


@pytest.fixture
def sample_xlsx(tmp_path, sample_df):
    """Write sample data to a temporary Excel file with two sheets."""
    path = tmp_path / "test_data.xlsx"
    with pd.ExcelWriter(path) as writer:
        sample_df.to_excel(writer, sheet_name="Building Info", index=False)
        # Second sheet with different data
        pd.DataFrame({
            "Component": ["Wall", "Roof", "Window"],
            "R-Value": [20.5, 30.0, 3.2],
        }).to_excel(writer, sheet_name="Envelope", index=False)
    return path


@pytest.fixture
def empty_registry():
    """A Registry with no mapping rules."""
    return Registry()
