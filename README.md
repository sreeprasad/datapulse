# DataPulse

Natural-language data explorer (local, reproducible).
📊 Catalog local CSV/Parquet/SQLite files, run SQL via DuckDB, and auto-generate notebooks.
🧠 Designed to pair with Codex CLI + GPT-5 for English → SQL → Notebook workflows.

## Features

📂 Catalog datasets once and reuse them (.datapulse/catalog.json).
🐼 Query local data with Pandas or DuckDB SQL.
📓 Generate Notebooks that auto-register datasets, run queries, export CSV, and plot.
🔒 Local & reproducible: your data stays on disk, notebooks are self-contained.
🤝 Codex CLI integration: ask GPT-5 for queries in plain English.

## Quick start

```bash

uv run datapulse version

uv run datapulse add sales data/sales.csv

uv run datapulse head sales --limit 3

uv run datapulse sql "SELECT product, SUM(qty*price) AS revenue FROM sales GROUP BY product ORDER BY revenue DESC"

uv run datapulse notebook --sql "SELECT COUNT(*) AS n FROM sales" --out notebooks/analysis.ipynb
```


## Use with Codex CLI

Turn plain English into runnable SQL:
```shell
codex -m gpt-5 -c model_reasoning_effort=high \
"I have a dataset registered as 'sales' with columns: order_id, customer, product, qty, price, date.
Write DuckDB SQL to compute the top 5 products by total revenue (qty*price), descending."
```

Then run the result:
```shell
uv run datapulse sql "<PASTE_SQL_FROM_CODEX>"
```

Generate a notebook with the query:
```shell
uv run datapulse notebook --sql "<PASTE_SQL_FROM_CODEX>" --out notebooks/revenue_top5.ipynb
```

## Project layout
```plaintext
datapulse/
  datapulse/       # CLI, engine, notebook generator
  data/            # sample datasets
  notebooks/       # generated notebooks
  tests/           # pytest files
```

## License
MIT






