[![CI](https://github.com/Chilanguiux/ETL/actions/workflows/ci.yml/badge.svg)](https://github.com/Chilanguiux/ETL/actions/workflows/ci.yml)

# Sales Pipeline - ETL + KPIs + API

ETL in Python 3 using Pandas/NumPy. Cleans, transforms, validates, and aggregates sales data. Exposes KPIs via FastAPI.

### Quick start - Create and activate a virtual env

#### Windows (PowerShell)

```python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

#### macOS/Linux

```python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Generate sample data, run ETL, print KPIs

### make sure the source is on PYTHONPATH

```
export PYTHONPATH=src # PowerShell: $env:PYTHONPATH="src"

python -m sales_etl.cli gen-sample --out data/raw/sales_data.csv --n 500
python -m sales_etl.cli etl data/raw/sales_data.csv --out data/processed/sales.parquet
python -m sales_etl.cli kpis data/processed/sales.parquet
```

#### Run the API

```
uvicorn sales_etl.api:app --reload
```

#### Endpoints:

GET /health

GET /kpis/by-month?parquet=data/processed/sales.parquet

GET /kpis/top-products?parquet=data/processed/sales.parquet&k=10

GET /kpis/regions?parquet=data/processed/sales.parquet

GET /kpis/outliers?parquet=data/processed/sales.parquet&z=3.0

#### Example:

```
curl "http://127.0.0.1:8000/kpis/top-products?parquet=data/processed/sales.parquet&k=5"
```

#### Project structure

```
sales-pipeline/
    data/
    raw/
    processed/
    src/
sales_etl/
api.py
cli.py
etl.py
metrics.py
validators.py
```

### Testing pytest -q

Troubleshooting

Use the same interpreter for install and run:

```
python -m pip install -r requirements.txt
```

#### If Windows blocks venv activation:

```
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

If CSVs come from Excel with semicolons or BOM, the loader handles it. If you pasted "pretty printed" tables, re-generate with:

```
python -m sales_etl.cli gen-sample --out data/raw/sales_data.csv
```

If you have any question about running this utility please DM me.

## License

MIT - see [LICENSE](LICENSE).
