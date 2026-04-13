"""Tests for ingestion pipeline."""

from murb_db.ingest import (
    cast_columns,
    clean_column_name,
    compute_file_hash,
    deduplicate_column_names,
    detect_column_types,
    ingest_file,
)
from murb_db.query import list_tables, query


def test_clean_column_name_basic():
    assert clean_column_name("Building Name") == "building_name"
    assert clean_column_name("Total Energy (kWh)") == "total_energy_kwh"
    assert clean_column_name("  Year Built  ") == "year_built"
    assert clean_column_name("R-Value") == "r_value"


def test_clean_column_name_special():
    assert clean_column_name("___weird___") == "weird"
    assert clean_column_name("123 number") == "123_number"
    assert clean_column_name("") == "unnamed"


def test_deduplicate_column_names():
    result = deduplicate_column_names(["a", "b", "a", "a", "c"])
    assert result == ["a", "b", "a_2", "a_3", "c"]


def test_detect_column_types(sample_df):
    types = detect_column_types(sample_df)
    assert types["Building Name"] == "TEXT"
    assert types["Year Built"] == "INTEGER"
    assert types["Total Energy (kWh)"] == "REAL"
    assert types["MURB Type"] == "TEXT"


def test_cast_columns(sample_df):
    types = detect_column_types(sample_df)
    casted = cast_columns(sample_df, types)
    assert casted["Year Built"].dtype.kind in ("i", "f")  # numeric


def test_compute_file_hash(sample_xlsx):
    h1 = compute_file_hash(sample_xlsx)
    h2 = compute_file_hash(sample_xlsx)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_ingest_file_end_to_end(tmp_db, sample_xlsx, empty_registry):
    tables = ingest_file(sample_xlsx, tmp_db, empty_registry)
    assert len(tables) == 2  # two sheets

    all_tables = list_tables(conn=tmp_db)
    assert len(all_tables) == 2

    # Check data was loaded
    for tbl in all_tables:
        df = query(f"SELECT * FROM [{tbl}]", conn=tmp_db)
        assert len(df) > 0

    # Check sources were recorded
    sources = query("SELECT * FROM _sources", conn=tmp_db)
    assert len(sources) == 2

    # Check metadata was recorded
    meta = query("SELECT * FROM _table_metadata", conn=tmp_db)
    assert len(meta) > 0


def test_ingest_dedup(tmp_db, sample_xlsx, empty_registry):
    ingest_file(sample_xlsx, tmp_db, empty_registry)
    tables2 = ingest_file(sample_xlsx, tmp_db, empty_registry)
    assert tables2 == []  # skipped due to same hash


def test_ingest_force(tmp_db, sample_xlsx, empty_registry):
    ingest_file(sample_xlsx, tmp_db, empty_registry)
    tables2 = ingest_file(sample_xlsx, tmp_db, empty_registry, force=True)
    assert len(tables2) == 2  # re-ingested
