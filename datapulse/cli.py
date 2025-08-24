import click
from rich.console import Console
from rich.table import Table

from . import engine
from . import notebook as nb


console = Console()


@click.group()
def cli():
    pass


@cli.command()
def version():
    click.echo("DataPulse 0.1.0")


@cli.command("add")
@click.argument("name")
@click.argument("path", type=click.Path(exists=True))
def add_cmd(name, path):
    """Add a dataset to the catalog."""
    try:
        engine.add_dataset(name, path)
        console.print(f":white_check_mark: Added [bold]{name}[/bold] -> {path}")
    except Exception as e:
        console.print(f":x: [red]{e}[/red]", highlight=False)
        raise SystemExit(1)


@cli.command("ls")
def ls_cmd():
    """List cataloged datasets."""
    rows = engine.list_datasets()
    if not rows:
        console.print("No datasets. Add one with: datapulse add <name> <path>")
        return
    table = Table(title="DataPulse Catalog")
    table.add_column("Name", style="bold")
    table.add_column("Format")
    table.add_column("Path")
    for r in rows:
        table.add_row(r["name"], r["format"], r["path"])
    console.print(table)


@cli.command("head")
@click.argument("name")
@click.option("--table", default=None, help="For SQLite DBs, a specific table to preview.")
@click.option("--limit", default=5, show_default=True, type=int)
def head_cmd(name, table, limit):
    """Preview the top rows of a dataset."""
    try:
        df = engine.load_df(name, table=table, limit=limit)
        # Pretty print small tables
        console.print(df)
    except Exception as e:
        console.print(f":x: [red]{e}[/red]")
        raise SystemExit(1)


@cli.command("sql")
@click.argument("query", nargs=-1)
def sql_cmd(query):
    """Run a SQL query across all cataloged datasets (DuckDB in-memory)."""
    sql = " ".join(query).strip()
    if not sql:
        console.print(":warning: Provide a query, e.g.: datapulse sql \"SELECT COUNT(*) FROM sales\"")
        raise SystemExit(2)
    try:
        df = engine.run_sql(sql)
        console.print(df)
    except Exception as e:
        console.print(f":x: [red]{e}[/red]")
        raise SystemExit(1)


@cli.command("notebook")
@click.option("--sql", "sql_text", required=True, help="SQL to embed in the generated notebook.")
@click.option("--out", "out_path", default="notebooks/analysis.ipynb", show_default=True)
def notebook_cmd(sql_text, out_path):
    """Generate a reproducible Jupyter notebook for a SQL query."""
    try:
        out = nb.write_notebook_from_sql(sql_text, out_path=out_path)
        console.print(f":white_check_mark: Notebook written to [bold]{out}[/bold]")
        console.print("Open it with:  jupyter lab notebooks/analysis.ipynb")
    except Exception as e:
        console.print(f":x: [red]{e}[/red]")
        raise SystemExit(1)

