"""
Custom parsers for the RDH Halifax MURB workbook.

This file contains tailored ingestion logic for each sheet type in the
'Original File - HX only.xlsx' workbook. The workbook is an engineering
costing model, not a flat database — sheets have multi-row headers,
side-by-side data blocks, key-value sections, and merged cells.

Database tables produced:
    energy_scenarios        — 1728 model scenarios with EUI, GHGI, costs, end-use breakdowns
    lcca_results            — LCCA results for each scenario x 16 option combos
    lcca_scenarios          — The 16 LCCA option combinations (PV, DHW HR, window, vent)
    ecm_line_items          — Line-item costs from all ECM sheets (windows, walls, HVAC, etc.)
    building_info           — Key-value building characteristics
    system_cost_summary     — Summary costs per ECM category from the System Costs sheet
    lcca_assumptions        — LCCA financial assumptions
    city_cost_index         — Regional cost adjustment factors
    reference_models        — The 9 reference/baseline models (rows 4-12 of Energy Results)
"""

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from murb_db.ingest import clean_column_name, compute_file_hash
from murb_db.schema import init_db


def _record_source(conn: sqlite3.Connection, file_path: str, file_hash: str,
                   sheet_name: str, table_name: str, row_count: int) -> int:
    """Insert a _sources record and return the source_id."""
    cursor = conn.execute(
        "INSERT INTO _sources (file_path, file_hash, sheet_name, table_name, row_count) VALUES (?,?,?,?,?)",
        (file_path, file_hash, sheet_name, table_name, row_count),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# 1. Energy Results & Costing
# ---------------------------------------------------------------------------

# Column mapping for Energy Results section 2 (row 19 = headers, data starts row 20)
_ENERGY_COLS = {
    0: "scenario_id",
    1: "tier",
    2: "pct_energy_savings",
    3: "reference_model",
    5: "hvac_system",
    6: "dhw_system_type",
    7: "walls",
    8: "windows",
    9: "airtightness",
    11: "teui_adjusted_kwh_m2a",
    12: "teui_before_pv_kwh_m2a",
    13: "tedi_kwh_m2a",
    14: "ghgi_kg_co2e_m2a",
    15: "gas_eui_before_hr_kwh_m2a",
    16: "electricity_eui_kwh_m2a",
    17: "pv_generation_eui_kwh_m2a",
    19: "heating_cooling_system",
    20: "demand_scenario",
    22: "mau_heating_cooling_by",
    23: "mau_size",
    25: "enclosure_cost",
    26: "heating_cooling_system_cost",
    27: "dhw_system_cost",
    28: "ventilation_system_cost",
    29: "total_cost",
    30: "energy_cost_first_year",
    31: "npv",
    33: "energy_savings_cost",
    34: "incremental_npv",
    36: "cost_index_enclosure",
    37: "cost_index_heating_cooling",
    38: "cost_index_dhw",
    39: "cost_index_ventilation",
    40: "cost_index_total",
    42: "gas_eui_heating",
    43: "gas_eui_dhw_no_hr",
    44: "gas_eui_dhw_with_hr",
    45: "elec_eui_heating",
    46: "elec_eui_dhw_no_hr",
    47: "elec_eui_dhw_with_hr",
    48: "elec_eui_cooling",
    49: "elec_eui_lighting",
    50: "elec_eui_plug_loads",
    51: "elec_eui_pumps",
    52: "elec_eui_fans",
    53: "elec_eui_heat_rejection",
    54: "elec_eui_process",
    56: "lcca_code",
    57: "with_dhw_heat_recovery",
    59: "elec_kwh_with_hr",
    60: "gas_kwh_with_hr",
    61: "elec_kwh_without_hr",
    62: "gas_kwh_without_hr",
    63: "window_code",
}

# Reference models: section 1 (rows 4-12, cols 0-43)
# Ref models do NOT have enclosure_cost (col 22) or total_cost (col 26) — those are NaN
_REF_MODEL_COLS = {
    0: "ref_model_id",
    1: "teui",
    2: "tedi",
    3: "ghgi",
    5: "hvac_system",
    6: "dhw_system_type",
    7: "walls",
    8: "windows",
    9: "airtightness",
    11: "gas_eui",
    13: "electricity_eui",
    15: "heating_cooling_system",
    16: "demand_scenario",
    19: "mau_heating_cooling_by",
    20: "mau_size",
    23: "heating_cooling_system_cost",
    24: "dhw_system_cost",
    25: "ventilation_system_cost",
    28: "gas_eui_heating",
    29: "gas_eui_dhw",
    35: "elec_eui_heating",
    36: "elec_eui_dhw",
    37: "elec_eui_cooling",
    38: "elec_eui_lighting",
    39: "elec_eui_plug_loads",
    40: "elec_eui_pumps",
    41: "elec_eui_fans",
    42: "elec_eui_heat_rejection",
    43: "elec_eui_process",
}


def parse_energy_results(xls: pd.ExcelFile) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the 'Energy Results & Costing - HX' sheet.

    Returns (energy_scenarios_df, reference_models_df).
    """
    df = xls.parse("Energy Results & Costing -  HX", header=None)

    # --- Reference models (rows 4-12) ---
    ref_rows = []
    for i in range(4, 13):
        row_data = {}
        for col_idx, col_name in _REF_MODEL_COLS.items():
            if col_idx < df.shape[1]:
                val = df.iloc[i, col_idx]
                row_data[col_name] = val if pd.notna(val) else None
        if row_data.get("ref_model_id") is not None:
            ref_rows.append(row_data)
    ref_df = pd.DataFrame(ref_rows)

    # Forward-fill merged-cell gaps in reference models.
    # The spreadsheet groups models in 3s (FCU: 1-3, PTHP: 4-6, WLHP: 7-9)
    # where only the first row of each group has the HVAC system name, walls, etc.
    ffill_cols = ["hvac_system", "walls", "windows", "airtightness",
                  "heating_cooling_system", "demand_scenario",
                  "mau_heating_cooling_by", "mau_size"]
    for col in ffill_cols:
        if col in ref_df.columns:
            ref_df[col] = ref_df[col].ffill()

    # --- Energy scenarios (rows 20 onward) ---
    scenario_rows = []
    for i in range(20, len(df)):
        val0 = df.iloc[i, 0]
        if pd.isna(val0):
            continue
        row_data = {}
        for col_idx, col_name in _ENERGY_COLS.items():
            if col_idx < df.shape[1]:
                val = df.iloc[i, col_idx]
                row_data[col_name] = val if pd.notna(val) else None
        scenario_rows.append(row_data)
    scenarios_df = pd.DataFrame(scenario_rows)

    # Coerce numeric columns
    numeric_cols = [c for c in scenarios_df.columns if c not in (
        "scenario_id", "hvac_system", "dhw_system_type", "walls", "windows",
        "airtightness", "heating_cooling_system", "demand_scenario",
        "mau_heating_cooling_by", "mau_size", "lcca_code",
        "with_dhw_heat_recovery", "window_code",
    )]
    for col in numeric_cols:
        if col in scenarios_df.columns:
            scenarios_df[col] = pd.to_numeric(scenarios_df[col], errors="coerce")

    # --- Fill GSHP/WLHP/WLHP+ HVAC cost gaps ---
    # Step 1: Try the 'lookups for energycosting' sheet (rows 0-9)
    hvac_cost_map = {}  # (system, demand) -> cost
    if "lookups for energycosting" in xls.sheet_names:
        lookup_df = xls.parse("lookups for energycosting", header=None)
        demand_cols = {1: "High", 2: "Medium", 3: "Low"}
        for row_i in range(10):
            sys_name = lookup_df.iloc[row_i, 0]
            if pd.isna(sys_name):
                continue
            sys_name = str(sys_name).strip()
            for col_j, demand in demand_cols.items():
                v = lookup_df.iloc[row_i, col_j]
                if pd.notna(v):
                    try:
                        hvac_cost_map[(sys_name, demand)] = float(v)
                    except (ValueError, TypeError):
                        pass

    # Step 2: Compute missing costs from ECM line-item sheets.
    # GSHP (4.5), WLHP (4.6) have line items but no totals in the summary.
    # Formula: total = (sum_material + sum_labour) * 1.1  (10% general conditions)
    _ecm_demand_map = {
        # Maps ECM variant substrings to demand scenario labels
        "high": "High", "medium": "Medium", "low": "Low",
    }
    _ecm_system_map = {
        "gshp_plant": "GSHP",
        "gas_boiler_wlhp": "WLHP",
    }
    gen_conditions_factor = 1.1  # from System Costs row 2

    for ecm_cat, sys_label in _ecm_system_map.items():
        if ecm_cat not in [s for s in xls.sheet_names]:
            pass  # We'll compute from the parsed ECM line items below

    # Parse ECM sheets to get totals (we already called parse_ecm_sheets later,
    # so instead just directly compute from the raw sheets here)
    for sheet_name, ecm_cat, sys_label in [
        ("4.5 GSHP Plant", "gshp_plant", "GSHP"),
        ("4.6 Gas Boiler + WLHPs", "gas_boiler_wlhp", "WLHP"),
    ]:
        if sheet_name not in xls.sheet_names:
            continue
        ecm_df = xls.parse(sheet_name, header=None)

        # Find option sections: "Option N - <Demand> Capacity" followed by "Total" then data
        current_demand = None
        header_row = None
        mat_col = None
        lab_col = None
        total_mat = 0.0
        total_lab = 0.0

        for i in range(len(ecm_df)):
            val0 = str(ecm_df.iloc[i, 0]).strip() if pd.notna(ecm_df.iloc[i, 0]) else ""
            val1 = str(ecm_df.iloc[i, 1]).strip() if ecm_df.shape[1] > 1 and pd.notna(ecm_df.iloc[i, 1]) else ""

            if "Option" in val0 or "Option" in val1:
                # Save previous section
                if current_demand and total_mat > 0:
                    cost = (total_mat + total_lab) * gen_conditions_factor
                    hvac_cost_map[(sys_label, current_demand)] = cost
                # Detect demand from label
                label = val0 + " " + val1
                current_demand = None
                for kw, dem in _ecm_demand_map.items():
                    if kw in label.lower():
                        current_demand = dem
                        break
                total_mat = 0.0
                total_lab = 0.0
                header_row = None
                continue

            # Find header row with "Material Rate" or "Material Cost"
            row_str = " ".join(str(ecm_df.iloc[i, j]) for j in range(min(12, ecm_df.shape[1])) if pd.notna(ecm_df.iloc[i, j]))
            if "Material Cost" in row_str and header_row is None:
                for j in range(ecm_df.shape[1]):
                    v = str(ecm_df.iloc[i, j]).strip() if pd.notna(ecm_df.iloc[i, j]) else ""
                    if v == "Material Cost":
                        mat_col = j
                    elif v == "Labour Cost":
                        lab_col = j
                header_row = i
                continue

            # Data rows
            if header_row is not None and current_demand and mat_col is not None:
                mat_v = ecm_df.iloc[i, mat_col] if mat_col < ecm_df.shape[1] else None
                lab_v = ecm_df.iloc[i, lab_col] if lab_col and lab_col < ecm_df.shape[1] else None
                if pd.notna(mat_v):
                    try:
                        total_mat += float(mat_v)
                    except (ValueError, TypeError):
                        pass
                if pd.notna(lab_v):
                    try:
                        total_lab += float(lab_v)
                    except (ValueError, TypeError):
                        pass

        # Save last section
        if current_demand and total_mat > 0:
            cost = (total_mat + total_lab) * gen_conditions_factor
            hvac_cost_map[(sys_label, current_demand)] = cost

    # FCU+/WLHP+ use the same HVAC costs as FCU/WLHP respectively
    for demand in ["High", "Medium", "Low"]:
        if ("FCU", demand) in hvac_cost_map and ("FCU+", demand) not in hvac_cost_map:
            hvac_cost_map[("FCU+", demand)] = hvac_cost_map[("FCU", demand)]
        if ("WLHP", demand) in hvac_cost_map and ("WLHP+", demand) not in hvac_cost_map:
            hvac_cost_map[("WLHP+", demand)] = hvac_cost_map[("WLHP", demand)]

    # Apply cost lookup to fill gaps
    mask = scenarios_df["heating_cooling_system_cost"].isna()
    if mask.any():
        for idx in scenarios_df[mask].index:
            sys_key = scenarios_df.loc[idx, "heating_cooling_system"]
            dem_key = scenarios_df.loc[idx, "demand_scenario"]
            cost = hvac_cost_map.get((sys_key, dem_key))
            if cost is not None:
                scenarios_df.loc[idx, "heating_cooling_system_cost"] = cost

    # Compute total_cost where missing: sum of enclosure + hvac + dhw + ventilation
    mask = scenarios_df["total_cost"].isna()
    if mask.any():
        components = ["enclosure_cost", "heating_cooling_system_cost",
                      "dhw_system_cost", "ventilation_system_cost"]
        for idx in scenarios_df[mask].index:
            vals = [scenarios_df.loc[idx, c] for c in components]
            if all(pd.notna(v) for v in vals):
                scenarios_df.loc[idx, "total_cost"] = sum(vals)

    # Drop columns that are 100% null (e.g. cost_index columns that are empty in source)
    all_null_cols = [c for c in scenarios_df.columns if scenarios_df[c].isna().all()]
    if all_null_cols:
        scenarios_df = scenarios_df.drop(columns=all_null_cols)

    # Fill missing HVAC costs in reference_models using the same cost map
    if "heating_cooling_system_cost" in ref_df.columns:
        mask = ref_df["heating_cooling_system_cost"].isna()
        if mask.any():
            for idx in ref_df[mask].index:
                sys_key = ref_df.loc[idx, "heating_cooling_system"]
                dem_key = ref_df.loc[idx, "demand_scenario"]
                cost = hvac_cost_map.get((sys_key, dem_key))
                if cost is not None:
                    ref_df.loc[idx, "heating_cooling_system_cost"] = cost

    return scenarios_df, ref_df


# ---------------------------------------------------------------------------
# 2. LCCA
# ---------------------------------------------------------------------------

def parse_lcca(xls: pd.ExcelFile) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Parse the 'LCCA - HX' sheet.

    Returns (lcca_results_df, lcca_scenarios_df).

    The sheet has 16 repeating blocks (LCCA1-LCCA16), each with the same
    8 columns: Description, Project Cost, Baseline Cost, Incremental Cost,
    Net Present Value, Incremental NPV, Energy Cost (1st Year), Energy Cost Savings.
    """
    df = xls.parse("LCCA - HX", header=None)

    # --- LCCA scenario definitions (rows 9-24, cols 0-4) ---
    scenario_rows = []
    for i in range(9, 25):
        scenario_rows.append({
            "lcca_id": str(df.iloc[i, 4]) if pd.notna(df.iloc[i, 4]) else None,
            "pv_generation": str(df.iloc[i, 0]) if pd.notna(df.iloc[i, 0]) else None,
            "dhw_heat_recovery": str(df.iloc[i, 1]) if pd.notna(df.iloc[i, 1]) else None,
            "window_type": str(df.iloc[i, 2]) if pd.notna(df.iloc[i, 2]) else None,
            "ventilation_hr": str(df.iloc[i, 3]) if pd.notna(df.iloc[i, 3]) else None,
        })
    lcca_scenarios_df = pd.DataFrame(scenario_rows)

    # --- LCCA data blocks ---
    # Each block is 10 columns wide, data columns at offset +6 from block start
    # Block starts: col 6(LCCA1), 16(LCCA2), 26(LCCA3), ... = 6 + 10*n
    lcca_col_names = [
        "description", "project_cost", "baseline_cost", "incremental_cost",
        "npv", "incremental_npv", "energy_cost_first_year", "energy_cost_savings_50yr",
    ]

    all_rows = []
    for block_idx in range(16):
        lcca_id = f"LCCA{block_idx + 1}"
        col_start = 6 + block_idx * 10  # 6, 16, 26, ...

        for i in range(6, len(df)):
            desc_val = df.iloc[i, col_start] if col_start < df.shape[1] else None
            if pd.isna(desc_val):
                continue

            row_data = {"lcca_id": lcca_id}
            for j, col_name in enumerate(lcca_col_names):
                cidx = col_start + j
                if cidx < df.shape[1]:
                    val = df.iloc[i, cidx]
                    row_data[col_name] = val if pd.notna(val) else None
                else:
                    row_data[col_name] = None
            # Use the row index from the energy scenarios as a join key
            # Row 6 in LCCA = "Halifax Typical" baseline, data rows start at 6
            row_data["lcca_row"] = i
            all_rows.append(row_data)

    lcca_df = pd.DataFrame(all_rows)

    # Coerce numeric
    for col in lcca_col_names[1:]:  # skip description
        if col in lcca_df.columns:
            lcca_df[col] = pd.to_numeric(lcca_df[col], errors="coerce")

    return lcca_df, lcca_scenarios_df


# ---------------------------------------------------------------------------
# 3. ECM Cost Sheets
# ---------------------------------------------------------------------------

# All ECM sheets share this pattern:
#   Row 0: blank
#   Row 1: ECM title + notes
#   Row 2: Total row (some sheets)
#   Row 3 or 4: Column headers (Commentary, Category, Item, Units, Quantity, ...)
#   Rows below: line items

_ECM_SHEETS = {
    "1 Windows":             "windows",
    "2 Walls":               "walls",
    "3 AT":                  "airtightness",
    "4.1 PTHP":              "pthp",
    "4.2 Boiler + Chiller":  "boiler_chiller",
    "4.3 VRF":               "vrf",
    "4.4 ASHP Plant":        "ashp_plant",
    "4.5 GSHP Plant":        "gshp_plant",
    "4.6 Gas Boiler + WLHPs": "gas_boiler_wlhp",
    "5 DHW":                 "dhw",
    "6.1 Vent wout HR":      "vent_no_hr",
    "6.2 Vent w HR":         "vent_with_hr",
    "7.1 PV":                "pv",
    "7.2 DHW Heat Recovery":  "dhw_heat_recovery",
}


def parse_ecm_sheets(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse all ECM cost breakdown sheets into a single table.

    Returns a DataFrame with columns:
        ecm_category, ecm_variant, commentary, category, item, units,
        quantity, material_rate, material_cost, labour_rate, labour_cost,
        bare_total, markup, total_op
    """
    all_rows = []

    for sheet_name, ecm_tag in _ECM_SHEETS.items():
        if sheet_name not in xls.sheet_names:
            continue
        df = xls.parse(sheet_name, header=None)

        # Find header row (contains "Category" or "Item")
        header_row = None
        for i in range(min(10, len(df))):
            row_vals = [str(v).strip() for v in df.iloc[i].dropna().tolist()]
            if "Category" in row_vals or "Item" in row_vals:
                header_row = i
                break

        if header_row is None:
            continue

        # Find ECM variant sections (rows before header that have titles)
        # Parse data rows below header
        current_variant = ecm_tag
        for i in range(len(df)):
            # Check for variant/section headers
            if i < header_row:
                # Look for ECM title rows
                val0 = df.iloc[i, 0] if pd.notna(df.iloc[i, 0]) else ""
                val1 = df.iloc[i, 1] if df.shape[1] > 1 and pd.notna(df.iloc[i, 1]) else ""
                title = str(val0) + str(val1)
                if "ECM" in title or "Option" in title or "Scenario" in title:
                    current_variant = clean_column_name(title[:80])
                continue

            if i == header_row:
                # Capture header names
                headers = []
                for j in range(df.shape[1]):
                    v = df.iloc[i, j]
                    headers.append(clean_column_name(str(v)) if pd.notna(v) else f"col_{j}")
                continue

            # Data rows
            # Skip empty rows and section breaks
            non_null = df.iloc[i].dropna()
            if len(non_null) < 3:
                # Might be a new section header
                val0 = df.iloc[i, 0] if pd.notna(df.iloc[i, 0]) else ""
                val1 = df.iloc[i, 1] if df.shape[1] > 1 and pd.notna(df.iloc[i, 1]) else ""
                title = str(val0) + str(val1)
                if title.strip() and ("ECM" in title or "Option" in title or "Scenario" in title or "Total" in title):
                    if "Total" not in title:
                        current_variant = clean_column_name(title[:80])
                continue

            # Build row dict
            row_data = {"ecm_category": ecm_tag, "ecm_variant": current_variant, "sheet_name": sheet_name}
            for j in range(min(len(headers), df.shape[1])):
                val = df.iloc[i, j]
                row_data[headers[j]] = val if pd.notna(val) else None

            all_rows.append(row_data)

    if not all_rows:
        return pd.DataFrame()

    result = pd.DataFrame(all_rows)

    # Standardize common column names across sheets
    rename_map = {}
    for col in result.columns:
        if "quantity" in col and "user" in col:
            rename_map[col] = "quantity"
        elif col == "quantity_user_":
            rename_map[col] = "quantity"
    if rename_map:
        result = result.rename(columns=rename_map)

    # Drop columns that are >90% null (artifacts from inconsistent sheet widths)
    threshold = 0.9
    null_frac = result.isna().mean()
    drop_cols = null_frac[null_frac > threshold].index.tolist()
    if drop_cols:
        result = result.drop(columns=drop_cols)

    return result


# ---------------------------------------------------------------------------
# 4. Building Info
# ---------------------------------------------------------------------------

def parse_building_info(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse the Building Info sheet as key-value pairs.

    The sheet has multiple independent sections side by side.
    We extract the unit mix (cols 0-2) and building areas (cols 0-4, rows 10+).
    """
    df = xls.parse("Building Info", header=None)
    kv_rows = []

    # Unit mix (rows 1-6, cols 0-2) — row 0 is header ("Type", "# of units", "%")
    for i in range(1, 7):
        key = df.iloc[i, 0]
        val = df.iloc[i, 1]
        if pd.notna(key) and pd.notna(val):
            kv_rows.append({
                "section": "unit_mix",
                "key": str(key).strip(),
                "value": str(val).strip(),
                "unit": str(df.iloc[i, 2]).strip() if pd.notna(df.iloc[i, 2]) else None,
            })

    # Building areas (rows 10+, cols 0-4) — row 10 is header ("Area Type", ...)
    for i in range(11, len(df)):
        key = df.iloc[i, 0]
        val = df.iloc[i, 1]
        if pd.notna(key) and pd.notna(val):
            key_str = str(key).strip()
            if not key_str or key_str == "nan":
                continue
            unit = str(df.iloc[i, 2]).strip() if pd.notna(df.iloc[i, 2]) else None
            kv_rows.append({
                "section": "building_areas",
                "key": key_str,
                "value": str(val).strip(),
                "unit": unit,
            })

    return pd.DataFrame(kv_rows)


# ---------------------------------------------------------------------------
# 5. System Costs
# ---------------------------------------------------------------------------

def parse_system_costs(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse the System Costs sheet — ECM category cost summaries.

    Structure per system:
        Row A: system name in col 0, "System Details..." in col 1
        Row B: "Material" in col 5, "Labour" in col 6, "O&P" in col 7, etc.
        Row C: first option — system description in col 0, option_num in col 3, option_name in col 4
        Row D+: subsequent options — option_num in col 3, option_name in col 4
        Row E: "Average" in col 3
    Cost columns start at col 5: Material, Labour, O&P, General Conditions, Total, $/m2, ...
    """
    df = xls.parse("System Costs", header=None)
    rows = []

    current_category = None
    current_system = None
    awaiting_data = False

    for i in range(len(df)):
        val0 = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else ""

        # Detect ECM category headers
        if "ECM Category" in val0:
            current_category = val0
            awaiting_data = False
            continue

        # Detect system sub-headers (col 0 has name, col 1 has "System Details")
        val1 = str(df.iloc[i, 1]).strip() if df.shape[1] > 1 and pd.notna(df.iloc[i, 1]) else ""
        if val1 and "System Details" in val1:
            current_system = val0
            awaiting_data = False
            continue

        # Detect "Material" header row — can be in col 0 or col 5
        row_str = " ".join(str(df.iloc[i, j]) for j in range(min(10, df.shape[1])) if pd.notna(df.iloc[i, j]))
        if "Material" in row_str and "Labour" in row_str:
            awaiting_data = True
            continue

        # Data rows
        if awaiting_data and current_system:
            # Check col 3 for option number, col 4 for option name
            # (first row has system desc in col 0, option_num in col 3)
            col3 = df.iloc[i, 3] if df.shape[1] > 3 and pd.notna(df.iloc[i, 3]) else None
            col4 = df.iloc[i, 4] if df.shape[1] > 4 and pd.notna(df.iloc[i, 4]) else None

            if col3 is None:
                # Empty row — end of section
                awaiting_data = False
                continue

            col3_str = str(col3).strip()
            row_data = {
                "ecm_category": current_category,
                "system": current_system,
            }

            if col3_str == "Average":
                row_data["option_num"] = "Average"
                row_data["option_name"] = "Average"
                awaiting_data = False
            else:
                row_data["option_num"] = col3_str
                row_data["option_name"] = str(col4).strip() if col4 else val0 if val0 else None

            # Capture costs from col 5 onward
            cost_labels = ["material_cost", "labour_cost", "overhead_profit", "general_conditions",
                           "total_cost", "cost_per_m2", "cost_per_unit", "cost_per_capacity"]
            for j, label in enumerate(cost_labels):
                cidx = 5 + j
                if cidx < df.shape[1]:
                    v = df.iloc[i, cidx]
                    row_data[label] = v if pd.notna(v) else None

            # Also capture system description from col 0 if present
            if val0:
                row_data["system_description"] = val0

            rows.append(row_data)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 6. Reference tables
# ---------------------------------------------------------------------------

def parse_lcca_assumptions(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse LCCA Assumptions as key-value pairs."""
    df = xls.parse("LCCA Assumptions", header=None)
    rows = []
    current_section = "general"
    for i in range(len(df)):
        key = df.iloc[i, 0]
        val = df.iloc[i, 1]
        if pd.isna(key):
            continue
        key_str = str(key).strip()
        if pd.isna(val):
            # Section header
            current_section = clean_column_name(key_str)
            continue
        rows.append({
            "section": current_section,
            "parameter": key_str,
            "value": val,
        })
    return pd.DataFrame(rows)


def parse_city_cost_index(xls: pd.ExcelFile) -> pd.DataFrame:
    """Parse City Cost Index lookup table.

    Layout: col 0 is blank (merged cell), headers in row 0 cols 1-3,
    data in rows 1-4 cols 1-3.
    """
    df = xls.parse("City Cost Index", header=None)
    rows = []
    for i in range(1, len(df)):
        city = df.iloc[i, 1] if df.shape[1] > 1 and pd.notna(df.iloc[i, 1]) else None
        if city is None:
            continue
        rows.append({
            "city": str(city).strip(),
            "city_cost_index": df.iloc[i, 2] if df.shape[1] > 2 else None,
            "city_vs_vancouver": df.iloc[i, 3] if df.shape[1] > 3 else None,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Master ingestion function
# ---------------------------------------------------------------------------

def ingest_rdh_workbook(file_path: Path, conn: sqlite3.Connection, force: bool = False) -> List[str]:
    """
    Ingest the RDH Halifax MURB workbook using tailored parsers.

    Returns list of table names created/updated.
    """
    file_path = Path(file_path)
    file_hash = compute_file_hash(file_path)

    # Dedup check
    if not force:
        existing = conn.execute(
            "SELECT id FROM _sources WHERE file_hash = ?", (file_hash,)
        ).fetchone()
        if existing:
            print(f"  Skipped (already ingested): {file_path.name}")
            return []

    init_db(conn)
    xls = pd.ExcelFile(file_path)
    tables_created = []

    def _write_table(table_name: str, df: pd.DataFrame, sheet_name: str):
        if df.empty:
            return
        source_id = _record_source(conn, str(file_path), file_hash, sheet_name, table_name, len(df))
        df = df.copy()
        df["_source_id"] = source_id
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        tables_created.append(table_name)
        print(f"  {table_name}: {len(df)} rows")

    # 1. Energy Results
    print("Parsing Energy Results & Costing...")
    scenarios_df, ref_df = parse_energy_results(xls)
    _write_table("energy_scenarios", scenarios_df, "Energy Results & Costing -  HX")
    _write_table("reference_models", ref_df, "Energy Results & Costing -  HX")

    # 2. LCCA
    print("Parsing LCCA...")
    lcca_df, lcca_scenarios_df = parse_lcca(xls)
    _write_table("lcca_results", lcca_df, "LCCA - HX")
    _write_table("lcca_scenarios", lcca_scenarios_df, "LCCA - HX")

    # 3. ECM Costs
    print("Parsing ECM cost sheets...")
    ecm_df = parse_ecm_sheets(xls)
    _write_table("ecm_line_items", ecm_df, "ECM Sheets (1-7)")

    # 4. Building Info
    print("Parsing Building Info...")
    binfo_df = parse_building_info(xls)
    _write_table("building_info", binfo_df, "Building Info")

    # 5. System Costs
    print("Parsing System Costs...")
    syscost_df = parse_system_costs(xls)
    _write_table("system_cost_summary", syscost_df, "System Costs")

    # 6. Reference tables
    print("Parsing reference tables...")
    lcca_assump_df = parse_lcca_assumptions(xls)
    _write_table("lcca_assumptions", lcca_assump_df, "LCCA Assumptions")

    cci_df = parse_city_cost_index(xls)
    _write_table("city_cost_index", cci_df, "City Cost Index")

    conn.commit()
    return tables_created
