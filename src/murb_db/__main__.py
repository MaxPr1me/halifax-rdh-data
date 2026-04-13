"""CLI entry point for murb-db."""

from pathlib import Path

import click

from murb_db.config import DB_PATH, MAPPINGS_DIR, RAW_DIR, get_connection
from murb_db.schema import init_db


@click.group()
def cli():
    """murb-db — AI-ready database for MURB energy model data."""
    pass


@cli.command()
def init():
    """Initialize the database with system tables."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    init_db(conn)
    conn.close()
    click.echo(f"Database initialized at {DB_PATH}")


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--force", is_flag=True, help="Re-ingest even if file hash matches.")
@click.option("--mappings", type=click.Path(), default=None, help="Path to YAML mappings file.")
def ingest(file_path, force, mappings):
    """Ingest an Excel file into the database."""
    from murb_db.ingest import ingest_file
    from murb_db.registry import Registry

    mappings_path = Path(mappings) if mappings else MAPPINGS_DIR / "default.yaml"
    registry = Registry.from_yaml(mappings_path)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    init_db(conn)
    tables = ingest_file(Path(file_path), conn, registry, force=force)
    conn.close()

    if tables:
        click.echo(f"Done. Tables: {', '.join(tables)}")
    else:
        click.echo("No new data ingested.")


@cli.command("ingest-dir")
@click.argument("dir_path", type=click.Path(exists=True), default=str(RAW_DIR))
@click.option("--pattern", default="*.xlsx", help="Glob pattern for files.")
@click.option("--force", is_flag=True)
@click.option("--mappings", type=click.Path(), default=None)
def ingest_dir(dir_path, pattern, force, mappings):
    """Ingest all matching Excel files from a directory."""
    from murb_db.ingest import ingest_directory
    from murb_db.registry import Registry

    mappings_path = Path(mappings) if mappings else MAPPINGS_DIR / "default.yaml"
    registry = Registry.from_yaml(mappings_path)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    init_db(conn)
    tables = ingest_directory(Path(dir_path), conn, registry, pattern=pattern, force=force)
    conn.close()

    if tables:
        click.echo(f"Done. Tables: {', '.join(set(tables))}")
    else:
        click.echo("No new data ingested.")


@cli.command()
def tables():
    """List all data tables in the database."""
    from murb_db.query import list_tables

    conn = get_connection()
    tbls = list_tables(conn=conn)
    conn.close()
    if tbls:
        for t in tbls:
            click.echo(t)
    else:
        click.echo("No tables yet. Ingest some data first.")


@cli.command()
@click.argument("table_name")
def describe(table_name):
    """Show column info for a table."""
    from murb_db.query import describe_table

    conn = get_connection()
    df = describe_table(table_name, conn=conn)
    conn.close()
    click.echo(df.to_string(index=False))


@cli.command("query")
@click.argument("sql")
def run_query(sql):
    """Run a SQL query and print results."""
    from murb_db.query import query

    conn = get_connection()
    df = query(sql, conn=conn)
    conn.close()
    click.echo(df.to_string(index=False))


@cli.command("ingest-rdh")
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--force", is_flag=True, help="Re-ingest even if file hash matches.")
def ingest_rdh(file_path, force):
    """Ingest an RDH MURB workbook using tailored parsers."""
    from murb_db.parsers import ingest_rdh_workbook

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    init_db(conn)
    tables = ingest_rdh_workbook(Path(file_path), conn, force=force)
    conn.close()

    if tables:
        click.echo(f"\nDone. Tables: {', '.join(tables)}")
    else:
        click.echo("No new data ingested.")


@cli.command("export-dashboard")
def export_dashboard():
    """Export data to docs/ for the GitHub Pages dashboard."""
    from murb_db.export_dashboard import export_all

    click.echo("Exporting dashboard data...")
    export_all()


@cli.command("schema-summary")
def schema_summary():
    """Print a full schema summary (designed for AI consumption)."""
    from murb_db.query import get_schema_summary

    conn = get_connection()
    summary = get_schema_summary(conn=conn)
    conn.close()
    click.echo(summary)


if __name__ == "__main__":
    cli()
