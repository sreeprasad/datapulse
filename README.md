# DataPulse

Natural-language data explorer (local, reproducible).  
Catalog local CSV/Parquet/SQLite files, run SQL via DuckDB, and auto-generate notebooks.

## Quick start

```bash
# install dev deps (uv recommended)
uv run datapulse version

# add a dataset
uv run datapulse add sales data/sales.csv

# preview
uv run datapulse head sales --limit 3

# query (DuckDB over local files)
uv run datapulse sql "SELECT product, SUM(qty*price) AS revenue FROM sales GROUP BY product ORDER BY revenue DESC"

# generate a reproducible notebook
uv run datapulse notebook --sql "SELECT COUNT(*) AS n FROM sales" --out notebooks/analysis.ipynb

