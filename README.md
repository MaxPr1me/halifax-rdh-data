# Halifax RDH MURB Energy Database

A tool that converts complex RDH building engineering Excel workbooks into a clean, queryable SQLite database. Built for the Halifax MURB (Multi-Unit Residential Building) energy modelling project.

---

## What This Tool Does

RDH Building Engineering provides large Excel workbooks with energy model results, cost estimates, and life-cycle cost analyses for Halifax buildings. These workbooks have complex formatting (merged cells, multi-row headers, side-by-side data blocks) that makes them hard to work with directly.

This tool:
1. **Reads** the Excel workbook automatically
2. **Cleans** the data (handles merged cells, fixes headers, fills cost gaps)
3. **Stores** everything in a single SQLite database file (`data/processed/murb.db`)
4. **Lets you query** the data with simple commands or through a Jupyter notebook

---

## Quick Start (Step by Step)

### Step 1: Get the Code

If you already have the repository folder on your computer, skip to Step 2.

Otherwise, download it from GitHub:
- Go to **https://github.com/MaxPr1me/halifax-rdh-data**
- Click the green **"Code"** button, then **"Download ZIP"**
- Extract the ZIP to a folder on your computer (e.g., `C:\Users\YourName\halifax-rdh-data`)

### Step 2: Install Python

You need Python 3.9 or newer. To check if you have it:
1. Open **Command Prompt** (search "cmd" in the Start menu)
2. Type `python --version` and press Enter
3. If you see `Python 3.9.x` or higher, you're good. If not, download Python from [python.org](https://www.python.org/downloads/) and install it (check "Add to PATH" during install)

### Step 3: Set Up the Project

Open Command Prompt, navigate to the project folder, and run these commands one at a time:

```
cd C:\Users\YourName\halifax-rdh-data
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

What these do:
- `cd ...` — moves into the project folder
- `python -m venv .venv` — creates an isolated Python environment (so this project doesn't interfere with anything else)
- `.venv\Scripts\activate` — activates that environment (you'll see `(.venv)` appear in your prompt)
- `pip install -e ".[dev]"` — installs all the project's dependencies

**Every time you open a new Command Prompt to use this tool**, you need to activate the environment again:
```
cd C:\Users\YourName\halifax-rdh-data
.venv\Scripts\activate
```

### Step 4: Add Your Excel File

Place your RDH Excel workbook in the `excel_sheets/` folder. For example:
```
excel_sheets/Original File - HX only.xlsx
```

### Step 5: Build the Database

Run these two commands:

```
murb-db init
murb-db ingest-rdh "excel_sheets/Original File - HX only.xlsx"
```

- `murb-db init` — creates the empty database structure
- `murb-db ingest-rdh ...` — reads the Excel file and populates the database

You should see output listing the tables created and row counts (e.g., `energy_scenarios: 1728 rows`).

### Step 6: Explore the Data

**Option A — Command line (quick lookups):**

```
murb-db tables                          # list all tables
murb-db describe energy_scenarios       # show columns in a table
murb-db query "SELECT * FROM energy_scenarios LIMIT 5"
murb-db schema-summary                  # full schema overview
```

**Option B — Jupyter notebook (interactive exploration + charts):**

```
jupyter notebook notebooks/01_explore.ipynb
```

This opens a browser-based notebook with ready-made cells for browsing tables, running queries, and creating charts.

---

## Available Commands

| Command | What It Does |
|---------|-------------|
| `murb-db init` | Set up the database (run once) |
| `murb-db ingest-rdh <file>` | Load an RDH Excel workbook into the database |
| `murb-db ingest-rdh <file> --force` | Reload a file even if it was already ingested |
| `murb-db tables` | List all data tables |
| `murb-db describe <table>` | Show columns and types for a table |
| `murb-db query "<sql>"` | Run a SQL query and print results |
| `murb-db schema-summary` | Print the full database schema (useful for AI tools) |

---

## What's in the Database

After ingesting an RDH workbook, you'll have these tables:

| Table | Rows | Description |
|-------|------|-------------|
| `energy_scenarios` | 1,728 | All energy model scenarios — combinations of HVAC system, DHW, walls, windows, airtightness, and demand |
| `reference_models` | 9 | Baseline reference building models |
| `lcca_results` | ~28,000 | Life-cycle cost analysis results across 16 scenario combinations |
| `lcca_scenarios` | 16 | The 16 LCCA scenario definitions (PV, DHW recovery, window type, ventilation) |
| `ecm_line_items` | ~400 | Detailed cost breakdowns for energy conservation measures |
| `system_costs` | varies | HVAC system cost details |
| `building_info` | varies | Unit mix, areas, and building characteristics |
| `lcca_assumptions` | varies | Economic assumptions used in LCCA (rates, escalation, etc.) |
| `city_cost_index` | varies | Regional cost adjustment factors |

---

## Example Queries

**What's the cheapest HVAC option for high airtightness?**
```sql
SELECT hvac_system, MIN(total_cost) as min_cost
FROM energy_scenarios
WHERE airtightness = 'High'
GROUP BY hvac_system
ORDER BY min_cost;
```

**Compare total energy use across wall types:**
```sql
SELECT wall_type, AVG(total_energy_intensity) as avg_eui
FROM energy_scenarios
GROUP BY wall_type
ORDER BY avg_eui;
```

**List all GSHP scenarios with their costs:**
```sql
SELECT hvac_system, dhw_system, wall_type, window_type, total_cost
FROM energy_scenarios
WHERE hvac_system LIKE '%GSHP%'
ORDER BY total_cost;
```

---

## Folder Structure

```
halifax-rdh-data/
├── excel_sheets/          ← Put your Excel workbooks here (not uploaded to GitHub)
├── data/
│   ├── raw/               ← For any raw data files
│   └── processed/
│       └── murb.db        ← The SQLite database (created by murb-db init)
├── src/murb_db/           ← The Python source code
├── tests/                 ← Automated tests
├── notebooks/             ← Jupyter notebooks for exploration
├── schema_mappings/       ← Configuration for data mapping
└── pyproject.toml         ← Project dependencies and settings
```

---

## Troubleshooting

**"murb-db is not recognized"**
→ Make sure you activated the virtual environment (`.venv\Scripts\activate`)

**"No module named murb_db"**
→ Run `pip install -e ".[dev]"` again

**Database is empty after ingest**
→ Check that the Excel file path is correct and the file is in `excel_sheets/`

**"File already ingested, use --force"**
→ The file was already loaded. Use `murb-db ingest-rdh "file.xlsx" --force` to reload

---

## For AI / Claude Users

Run `murb-db schema-summary` to get a full database schema description that you can paste into any AI chat for help writing queries or generating visualizations. See `CLAUDE.md` for detailed agent instructions.
