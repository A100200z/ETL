install:
\tpip install -r requirements.txt
etl:
\tpython -m sales_etl.cli etl data/raw/*.csv --out data/processed/sales.parquet
kpis:
\tpython -m sales_etl.cli kpis data/processed/sales.parquet
api:
\tuvicorn sales_etl.api:app --reload
test:
\tpytest -q
lint:
\tflake8 sales_etl tests