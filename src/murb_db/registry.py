"""Schema mapping registry — maps Excel sheets to database tables via YAML config."""

import fnmatch
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml


@dataclass
class MappingRule:
    file_pattern: str          # e.g. "*.xlsx" or "project_summary*"
    sheet_name: str            # exact sheet name
    target_table: str          # resulting SQLite table name
    column_map: Dict[str, str] = field(default_factory=dict)  # original -> renamed
    primary_key: Optional[str] = None


class Registry:
    """Manages sheet-to-table mapping rules."""

    def __init__(self, rules: Optional[List[MappingRule]] = None):
        self.rules: List[MappingRule] = rules or []

    @classmethod
    def from_yaml(cls, path: Path) -> "Registry":
        path = Path(path)
        if not path.exists():
            return cls()
        with open(path) as f:
            data = yaml.safe_load(f)
        if not data or "mappings" not in data:
            return cls()
        rules = []
        for entry in data["mappings"]:
            rules.append(
                MappingRule(
                    file_pattern=entry.get("file_pattern", "*"),
                    sheet_name=entry["sheet_name"],
                    target_table=entry["target_table"],
                    column_map=entry.get("column_map", {}),
                    primary_key=entry.get("primary_key"),
                )
            )
        return cls(rules)

    def lookup(self, file_name: str, sheet_name: str) -> Optional[MappingRule]:
        for rule in self.rules:
            if fnmatch.fnmatch(file_name, rule.file_pattern) and rule.sheet_name == sheet_name:
                return rule
        return None

    def resolve_table_name(self, file_name: str, sheet_name: str) -> str:
        rule = self.lookup(file_name, sheet_name)
        if rule:
            return rule.target_table
        # Auto-generate: filestem__sheetname, cleaned
        from murb_db.ingest import clean_column_name
        stem = Path(file_name).stem
        return clean_column_name(f"{stem}__{sheet_name}")

    def resolve_column_map(self, file_name: str, sheet_name: str) -> Dict[str, str]:
        rule = self.lookup(file_name, sheet_name)
        return rule.column_map if rule else {}
