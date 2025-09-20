from fastapi import FastAPI, HTTPException
import pandas as pd
from pathlib import Path
from .metrics import (
    kpi_sales_by_month,
    kpi_top_products,
    kpi_region_stats,
    detect_outliers_zscore,
)

app = FastAPI(title="Sales KPIs API")


def _load(parquet_path: str):
    p = Path(parquet_path)
    if not p.exists():
        raise HTTPException(status_code=404, detail="dataset not found")
    return pd.read_parquet(p)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/kpis/by-month")
def by_month(parquet: str = "data/processed/sales.parquet"):
    df = _load(parquet)
    return kpi_sales_by_month(df).to_dict(orient="records")


@app.get("/kpis/top-products")
def top_products(parquet: str = "data/processed/sales.parquet", k: int = 10):
    df = _load(parquet)
    return kpi_top_products(df, k=k).to_dict(orient="records")


@app.get("/kpis/regions")
def regions(parquet: str = "data/processed/sales.parquet"):
    df = _load(parquet)
    return kpi_region_stats(df).to_dict(orient="records")


@app.get("/kpis/outliers")
def outliers(parquet: str = "data/processed/sales.parquet", z: float = 3.0):
    df = _load(parquet)
    return detect_outliers_zscore(df, z=z)[["date", "product", "revenue"]].to_dict(
        orient="records"
    )
