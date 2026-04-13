"""Tests for query helpers."""

from murb_db.query import (
    describe_table,
    get_schema_summary,
    list_tables,
    query,
    search_columns,
)
from murb_db.ingest import ingest_file


def test_query_returns_dataframe(tmp_db):
    df = query("SELECT 1 as val", conn=tmp_db)
    assert df["val"].iloc[0] == 1


def test_list_tables_excludes_system(tmp_db):
    tables = list_tables(conn=tmp_db)
    assert "_sources" not in tables
    assert "_table_metadata" not in tables


def test_describe_table_after_ingest(tmp_db, sample_xlsx, empty_registry):
    ingest_file(sample_xlsx, tmp_db, empty_registry)
    tables = list_tables(conn=tmp_db)
    assert len(tables) > 0

    df = describe_table(tables[0], conn=tmp_db)
    assert "column_name" in df.columns
    assert "column_type" in df.columns


def test_search_columns(tmp_db, sample_xlsx, empty_registry):
    ingest_file(sample_xlsx, tmp_db, empty_registry)
    results = search_columns("energy", conn=tmp_db)
    assert len(results) >= 1


def test_schema_summary_empty(tmp_db):
    summary = get_schema_summary(conn=tmp_db)
    assert "empty" in summary.lower()


def test_schema_summary_with_data(tmp_db, sample_xlsx, empty_registry):
    ingest_file(sample_xlsx, tmp_db, empty_registry)
    summary = get_schema_summary(conn=tmp_db)
    assert "Table:" in summary
    assert "rows" in summary
