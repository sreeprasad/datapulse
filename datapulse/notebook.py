from __future__ import annotations
import json
import pathlib
from datetime import datetime

import nbformat as nbf

CATALOG_DIR = pathlib.Path(".datapulse")
CATALOG_FILE = CATALOG_DIR / "catalog.json"


def _load_catalog() -> dict:
    if not CATALOG_FILE.exists():
        return {}
    return json.loads(CATALOG_FILE.read_text(encoding="utf-8"))

def _mk_markdown_cell(sql: str) -> nbf.NotebookNode:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    md = f"""# DataPulse Analysis

**Generated:** {ts}

**Query**
```sql
{sql.strip()}
```
This notebook auto-registers your cataloged datasets as DuckDB views, executes the SQL, and renders a quick preview + plot.
"""
    return nbf.v4.new_markdown_cell(md)


def _mk_setup_cell() -> nbf.NotebookNode:
    code = r"""import json, pathlib
import duckdb, pandas as pd
from IPython.display import display

CATALOG_DIR = pathlib.Path(".datapulse")
CATALOG_FILE = CATALOG_DIR / "catalog.json"

def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def register_catalog(con: duckdb.DuckDBPyConnection):
    if not CATALOG_FILE.exists():
        print("No catalog found (.datapulse/catalog.json). Add datasets with `datapulse add ...`")
        return
    catalog = json.loads(CATALOG_FILE.read_text(encoding="utf-8"))
    for ds_name, meta in catalog.items():
        path = meta["path"]
        fmt = meta["format"]
        view_name = ds_name
        if fmt == "csv":
            con.execute(f"CREATE OR REPLACE VIEW {_quote_ident(view_name)} AS SELECT * FROM read_csv_auto('{path}')")
        elif fmt == "parquet":
            con.execute(f"CREATE OR REPLACE VIEW {_quote_ident(view_name)} AS SELECT * FROM read_parquet('{path}')")
        elif fmt in {"sqlite", "db"}:
            schema = f"s_{ds_name}"
            con.execute(f"ATTACH '{path}' AS {_quote_ident(schema)} (TYPE SQLITE)")
        else:
            raise ValueError(f"Unsupported format in catalog: {fmt}")

con = duckdb.connect(database=":memory:")
register_catalog(con)
print("✅ DuckDB in-memory session ready; catalog views are registered.")
"""
    return nbf.v4.new_code_cell(code)



def _mk_sql_cell(sql: str) -> nbf.NotebookNode:
    code = f"""# Your SQL (editable)
sql = r\"\"\"{sql.strip()}\"\"\""""
    return nbf.v4.new_code_cell(code)


def _mk_execute_cell() -> nbf.NotebookNode:
    code = r"""# Execute query and show results
df = con.execute(sql).df()
print("Rows:", len(df))
display(df.head(10))

# Persist latest result for quick export
import pathlib
out_dir = pathlib.Path("notebooks")
out_dir.mkdir(parents=True, exist_ok=True)
df.to_csv(out_dir / "last_result.csv", index=False)
print("Saved:", out_dir / "last_result.csv")"""
    return nbf.v4.new_code_cell(code)


def _mk_plot_cell() -> nbf.NotebookNode:
    code = r"""# Try a quick plot (heuristic: use first categorical as x, first numeric as y)
import pandas as pd
import matplotlib.pyplot as plt

def pick_columns_for_plot(dataframe: pd.DataFrame):
    num_cols = [c for c in dataframe.columns if pd.api.types.is_numeric_dtype(dataframe[c])]
    cat_cols = [c for c in dataframe.columns if not pd.api.types.is_numeric_dtype(dataframe[c])]
    if num_cols and cat_cols:
        return cat_cols[0], num_cols[0]
    if len(num_cols) >= 2:
        return num_cols[0], num_cols[1]
    if len(df.columns) >= 2:
        return df.columns[0], df.columns[1]
    return None, None

x, y = pick_columns_for_plot(df)
if x is not None and y is not None:
    ax = df.groupby(x)[y].sum().plot(kind="bar", figsize=(8, 4))
    ax.set_title(f"{y} by {x}")
    plt.tight_layout()
    plt.show()
else:
    print("Not enough columns to auto-plot.")
"""
    return nbf.v4.new_code_cell(code)


def write_notebook_from_sql(sql: str, out_path: str = "notebooks/analysis.ipynb") -> pathlib.Path:
    nb = nbf.v4.new_notebook()
    nb.cells = [
        _mk_markdown_cell(sql),
        _mk_setup_cell(),
        _mk_sql_cell(sql),
        _mk_execute_cell(),
        _mk_plot_cell(),
    ]
    out = pathlib.Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, str(out))
    return out

