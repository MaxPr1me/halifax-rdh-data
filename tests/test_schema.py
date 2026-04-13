"""Tests for schema module."""

from murb_db.schema import (
    add_columns_if_missing,
    create_user_table,
    get_table_columns,
    init_db,
    table_exists,
    upsert_table_metadata,
)


def test_init_db_creates_system_tables(tmp_db):
    assert table_exists(tmp_db, "_sources")
    assert table_exists(tmp_db, "_table_metadata")


def test_create_user_table(tmp_db):
    columns = {"name": "TEXT", "value": "REAL"}
    create_user_table(tmp_db, "test_table", columns)
    assert table_exists(tmp_db, "test_table")
    cols = get_table_columns(tmp_db, "test_table")
    assert "id" in cols
    assert "name" in cols
    assert "value" in cols
    assert "_source_id" in cols


def test_add_columns_if_missing(tmp_db):
    create_user_table(tmp_db, "t1", {"a": "TEXT"})
    added = add_columns_if_missing(tmp_db, "t1", {"a": "TEXT", "b": "INTEGER", "c": "REAL"})
    assert set(added) == {"b", "c"}
    cols = get_table_columns(tmp_db, "t1")
    assert "b" in cols
    assert "c" in cols


def test_add_columns_no_duplicates(tmp_db):
    create_user_table(tmp_db, "t2", {"x": "TEXT"})
    added = add_columns_if_missing(tmp_db, "t2", {"x": "TEXT"})
    assert added == []


def test_upsert_table_metadata(tmp_db):
    col_info = [
        {"column_name": "energy", "column_type": "REAL", "original_name": "Energy (kWh)"},
    ]
    upsert_table_metadata(tmp_db, "buildings", col_info)
    row = tmp_db.execute(
        "SELECT * FROM _table_metadata WHERE table_name='buildings' AND column_name='energy'"
    ).fetchone()
    assert row is not None
    assert row["original_name"] == "Energy (kWh)"

    # Update description
    col_info[0]["column_type"] = "INTEGER"
    upsert_table_metadata(tmp_db, "buildings", col_info)
    row = tmp_db.execute(
        "SELECT * FROM _table_metadata WHERE table_name='buildings' AND column_name='energy'"
    ).fetchone()
    assert row["column_type"] == "INTEGER"
