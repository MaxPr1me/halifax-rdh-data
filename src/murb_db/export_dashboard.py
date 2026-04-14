"""Export database tables to docs/ for the GitHub Pages dashboard."""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from murb_db.config import DB_PATH, PROJECT_ROOT, get_connection


DOCS_DIR = PROJECT_ROOT / "docs"

# Tables to export (order doesn't matter)
EXPORT_TABLES = [
    "energy_scenarios",
    "reference_models",
    "lcca_results",
    "lcca_scenarios",
    "ecm_line_items",
    "system_cost_summary",
    "building_info",
    "lcca_assumptions",
    "city_cost_index",
]

# Columns to drop from export (internal-only)
DROP_COLS = {"_source_id", "lcca_row"}

# Filter dimensions in energy_scenarios (used for metadata)
FILTER_DIMS = [
    "necb_2020_ref_tier",
    "necb_2025_eui_tier",
    "heating_cooling_system",
    "demand_scenario",
    "dhw_system_type",
    "walls",
    "windows",
    "airtightness",
]

# Key numeric columns for metric ranges
METRIC_COLS = [
    "teui_adjusted_kwh_m2a",
    "tedi_kwh_m2a",
    "ghgi_kg_co2e_m2a",
    "pct_energy_savings",
    "total_cost",
    "energy_cost_first_year",
    "npv",
    "enclosure_cost",
    "heating_cooling_system_cost",
]


def _rows_to_dicts(conn, table):
    """Read a table into a list of plain dicts, dropping internal columns."""
    cur = conn.execute(f"SELECT * FROM [{table}]")
    cols = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    keep = [i for i, c in enumerate(cols) if c not in DROP_COLS]
    kept_cols = [cols[i] for i in keep]
    return [dict(zip(kept_cols, (row[i] for i in keep))) for row in rows]


def _build_metadata(conn):
    """Pre-compute filter metadata for the dashboard."""
    meta = {}

    # Distinct values per filter dimension
    dims = {}
    for dim in FILTER_DIMS:
        cur = conn.execute(
            f"SELECT DISTINCT [{dim}] FROM energy_scenarios ORDER BY [{dim}]"
        )
        dims[dim] = [row[0] for row in cur.fetchall()]
    meta["filter_dimensions"] = dims

    # Non-orthogonal mapping: which demand_scenarios exist per HVAC system
    cur = conn.execute(
        "SELECT heating_cooling_system, demand_scenario "
        "FROM energy_scenarios "
        "GROUP BY heating_cooling_system, demand_scenario "
        "ORDER BY heating_cooling_system, demand_scenario"
    )
    demand_by_hvac = {}
    for row in cur.fetchall():
        hvac, demand = row[0], row[1]
        demand_by_hvac.setdefault(hvac, []).append(demand)
    meta["demand_by_hvac"] = demand_by_hvac

    # Metric ranges (min/max for key numeric columns)
    ranges = {}
    for col in METRIC_COLS:
        cur = conn.execute(
            f"SELECT MIN([{col}]), MAX([{col}]) FROM energy_scenarios"
        )
        row = cur.fetchone()
        if row[0] is not None:
            ranges[col] = {"min": row[0], "max": row[1]}
    meta["metric_ranges"] = ranges

    meta["generated_at"] = datetime.now(timezone.utc).isoformat()
    return meta


def _process_lcca_results(rows):
    """Add integer scenario_id to lcca_results rows where description is numeric."""
    for row in rows:
        desc = row.get("description", "")
        try:
            row["scenario_id"] = int(desc)
        except (ValueError, TypeError):
            row["scenario_id"] = None
    return rows


def export_all():
    """Export all tables + metadata to docs/ for the dashboard."""
    conn = get_connection()

    # Build output
    data = {"metadata": _build_metadata(conn), "tables": {}}

    for table in EXPORT_TABLES:
        # Check table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        )
        if not cur.fetchone():
            print(f"  Skipping {table} (not found)")
            continue

        rows = _rows_to_dicts(conn, table)

        if table == "lcca_results":
            rows = _process_lcca_results(rows)

        data["tables"][table] = rows
        print(f"  {table}: {len(rows)} rows")

    conn.close()

    # Write files
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    json_path = DOCS_DIR / "data.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    size_mb = json_path.stat().st_size / (1024 * 1024)
    print(f"\n  data.json: {size_mb:.1f} MB")

    # Copy the SQLite database for the SQL query tab
    db_dest = DOCS_DIR / "murb.db"
    shutil.copy2(DB_PATH, db_dest)
    db_mb = db_dest.stat().st_size / (1024 * 1024)
    print(f"  murb.db:   {db_mb:.1f} MB")

    print(f"\nDashboard data exported to {DOCS_DIR}")
