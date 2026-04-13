"""murb_db — AI-ready database for Halifax RDH MURB energy model data."""

__version__ = "0.1.0"

from murb_db.query import query, list_tables, describe_table, search_columns, get_schema_summary
from murb_db.viz import bar_chart, scatter, timeseries
