from __future__ import annotations
import json
import os
import pathlib
from typing import Dict, List, Optional

import duckdb
import pandas as pd

CATALOG_DIR = pathlib.Path(".datapulse")
CATALOG_FILE = CATALOG_DIR / "catalog.json"

SUPPORTED_EXTS = {".csv", ".parquet", ".pq", ".sqlite", ".db"}

def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _ensure_catalog_dir() -> None:
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)


def _load_catalog() -> Dict[str, dict]:
    _ensure_catalog_dir()
    if not CATALOG_FILE.exists():
        return {}
    with CATALOG_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_catalog(catalog: Dict[str, dict]) -> None:
    _ensure_catalog_dir()
    with CATALOG_FILE.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, sort_keys=True)


def _infer_format(path: str) -> str:
    ext = pathlib.Path(path).suffix.lower()
    if ext in {".pq"}:
        ext = ".parquet"
    if ext not in SUPPORTED_EXTS:
        raise ValueError(f"Unsupported file extension '{ext}'. Supported: {sorted(SUPPORTED_EXTS)}")
    return ext.lstrip(".")


def add_dataset(name: str, path: str) -> None:
    """Register a local file under a logical name."""
    p = pathlib.Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    fmt = _infer_format(str(p))
    catalog = _load_catalog()
    catalog[name] = {"path": str(p), "format": fmt}
    _save_catalog(catalog)


def list_datasets() -> List[dict]:
    """Return catalog entries as a list of dicts: {name, path, format}."""
    catalog = _load_catalog()
    return [{"name": k, **v} for k, v in sorted(catalog.items())]


def remove_dataset(name: str) -> None:
    catalog = _load_catalog()
    if name in catalog:
        del catalog[name]
        _save_catalog(catalog)
    else:
        raise KeyError(f"No dataset named '{name}'")


def load_df(name: str, table: Optional[str] = None, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Materialize a dataset into a pandas DataFrame.
    """
    catalog = _load_catalog()
    if name not in catalog:
        raise KeyError(f"No dataset named '{name}'. Use `add_dataset(name, path)` first.")
    entry = catalog[name]
    fmt = entry["format"]
    path = entry["path"]

    if fmt == "csv":
        df = pd.read_csv(path)
        return df.head(limit) if limit else df

    if fmt == "parquet":
        df = pd.read_parquet(path)
        return df.head(limit) if limit else df

    if fmt in {"sqlite", "db"}:
        import sqlite3
        con = sqlite3.connect(path)
        try:
            if table is None:
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;", con)["name"].tolist()
                if not tables:
                    raise ValueError(f"No tables found in SQLite DB: {path}")
                table = tables[0]
            q = f"SELECT * FROM {table}"
            if limit:
                q += f" LIMIT {int(limit)}"
            return pd.read_sql(q, con)
        finally:
            con.close()

    raise ValueError(f"Unsupported format: {fmt}")


def run_sql(sql: str, register: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Execute SQL over local files via DuckDB.
    `register`: mapping of logical name -> optional table alias.
    All cataloged datasets are auto-attachable using DuckDB's 'read_csv'/'read_parquet' functions,
    but here we materialize them into DuckDB temp views for convenience.
    """

    con = duckdb.connect(database=":memory:")
    try:
        catalog = _load_catalog()

        for ds_name, meta in catalog.items():
            view_name = ds_name
            if register and ds_name in register and register[ds_name]:
                view_name = register[ds_name]
            path = meta["path"]
            fmt = meta["format"]
            if fmt == "csv":
                con.execute(f"CREATE VIEW {_quote_ident(view_name)} AS SELECT * FROM read_csv_auto('{path}')")
            elif fmt == "parquet":
                con.execute(f"CREATE VIEW {_quote_ident(view_name)} AS SELECT * FROM read_parquet('{path}')")
            elif fmt in {"sqlite", "db"}:
                schema = f"s_{ds_name}"
                con.execute(f"ATTACH '{path}' AS {_quote_ident(schema)} (TYPE SQLITE)")
            else:
                raise ValueError(f"Unsupported format in catalog: {fmt}")

        return con.execute(sql).df()
    finally:
        con.close()

