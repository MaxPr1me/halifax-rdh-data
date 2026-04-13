# CLAUDE.md — Agent Instructions for Halifax RDH MURB Database

This file tells Claude (or any AI agent) how to work with this project effectively.

---

## Project Overview

This is a **MURB (Multi-Unit Residential Building) energy modelling database** for Halifax buildings. RDH Building Engineering provides complex Excel workbooks with energy simulations, cost estimates, and life-cycle cost analyses. This tool converts those workbooks into a clean SQLite database for querying and visualization.

**Owner:** MaxPr1me (GitHub)
**Primary user:** Non-developer; keep all instructions, outputs, and suggestions beginner-friendly.

---

## Key Architecture

- **Language:** Python 3.9+
- **Package manager:** pip with venv (uv has SSL issues on this machine)
- **Database:** SQLite at `data/processed/murb.db`
- **CLI framework:** Click — entry point is `murb-db` (defined in `src/murb_db/__main__.py`)
- **Build system:** Hatchling
- **Core libraries:** pandas, openpyxl, matplotlib

### Source Code Layout

```
src/murb_db/
├── __init__.py     — Package exports (query, viz functions)
├── __main__.py     — Click CLI commands (init, ingest-rdh, tables, describe, query, schema-summary)
├── config.py       — Path constants (PROJECT_ROOT, DB_PATH, RAW_DIR, etc.) and get_connection()
├── parsers.py      — ★ CRITICAL: Custom parsers for RDH Excel workbooks (merged cells, multi-row headers, cost gap filling)
├── ingest.py       — Generic ingestion pipeline (clean columns, detect types, dedup, hash tracking)
├── schema.py       — DDL: creates _sources, _table_metadata; table creation helpers
├── query.py        — Query helpers: query(), list_tables(), describe_table(), search_columns(), get_schema_summary()
├── registry.py     — YAML-driven sheet-to-table mapping
└── viz.py          — Visualization wrappers: bar_chart(), scatter(), timeseries()
```

### Database Tables

After ingestion, the database contains:

| Table | ~Rows | Description |
|-------|-------|-------------|
| `energy_scenarios` | 1,728 | All scenario combinations: 8 HVAC × 3 DHW × 3 wall × 5 window × 3 airtight × ~3 demand |
| `reference_models` | 9 | Baseline reference building models |
| `lcca_results` | ~28,000 | Life-cycle cost results for 16 scenario combinations |
| `lcca_scenarios` | 16 | LCCA scenario definitions (PV, DHW recovery, windows, ventilation) |
| `ecm_line_items` | ~400 | Detailed ECM cost breakdowns (material + labour) |
| `system_costs` | varies | HVAC system cost summaries |
| `building_info` | varies | Unit mix and building area data |
| `lcca_assumptions` | varies | Economic assumptions (discount rates, escalation, etc.) |
| `city_cost_index` | varies | Regional cost adjustment factors |
| `_sources` | — | Provenance: file path, SHA-256 hash, sheet name, ingest timestamp |
| `_table_metadata` | — | Column descriptions and original names |

---

## Common Tasks

### Get a full schema summary (useful as context for AI queries)
```bash
murb-db schema-summary
```

### Run a SQL query
```bash
murb-db query "SELECT hvac_system, AVG(total_cost) FROM energy_scenarios GROUP BY hvac_system"
```

### Re-ingest a workbook after parser changes
```bash
murb-db ingest-rdh "excel_sheets/Original File - HX only.xlsx" --force
```

### Run tests
```bash
pytest
```

### Generate visualizations from Python
```python
from murb_db import query, bar_chart
df = query("SELECT hvac_system, AVG(total_energy_intensity) as avg_eui FROM energy_scenarios GROUP BY hvac_system")
bar_chart(df, x="hvac_system", y="avg_eui", title="Average EUI by HVAC System")
```

---

## Parser Details (parsers.py)

This is the most complex file. Key things to know:

- **Merged cells:** Excel merged cells only populate the first row. The parsers use `ffill()` to fill gaps in categorical columns (hvac_system, dhw_system, etc.).
- **Cost gap filling:** GSHP, WLHP, and WLHP+ systems don't have costs in the summary sheets. Costs are computed from ECM detail sheets using: `total = (sum_material + sum_labour) * 1.1` (10% general conditions factor).
- **LCCA blocks:** 16 repeating 10-column-wide blocks, each at column offset `6 + 10*n`.
- **Column mapping:** `_ENERGY_COLS` maps 52 Excel column indices to clean database column names. `_REF_MODEL_COLS` maps 29 columns for reference models.
- **ECM sheets:** 14 individual cost detail sheets parsed into a unified `ecm_line_items` table.

---

## Important Conventions

1. **Excel files go in `excel_sheets/`** — this folder is gitignored (raw data stays local)
2. **Database lives at `data/processed/murb.db`** — also gitignored
3. **Provenance tracking:** Every ingested file is hashed (SHA-256) and recorded in `_sources`. Re-ingesting the same file is skipped unless `--force` is used.
4. **AI-readability:** `get_schema_summary()` produces a text description of the full schema designed for LLM consumption.

---

## Updating This Project

When making changes to this repository:

- **Keep README.md up to date** — it is written for a non-technical user. Update it whenever commands, tables, or setup steps change.
- **Keep this CLAUDE.md up to date** — update it when architecture, tables, parsers, or conventions change.
- **Keep memory files up to date** — check `C:\Users\mastjacq\.claude\projects\c--Users-mastjacq-wd-halifax-rdh-data\memory\MEMORY.md` for project memory that should be refreshed when significant changes occur.

---

## Gotchas

- Python 3.11 doesn't work on this machine (uv SSL error) — stick with Python 3.9
- The `.python-version` file is set to `3.9`
- Cost columns in energy_scenarios: `enclosure_cost`, `heating_cooling_system_cost`, `total_cost` — the HVAC cost was derived from ECM sheets, not directly from the energy results sheet
- Reference models have no `enclosure_cost` or `total_cost` columns (genuinely absent from source data)
- The `_ENERGY_COLS` dict in parsers.py is the source of truth for column mapping from Excel indices to DB names
