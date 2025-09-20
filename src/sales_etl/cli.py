import json
import typer
from pathlib import Path
import pandas as pd
import numpy as np
from .etl import read_many, write_parquet
from .metrics import (
    kpi_sales_by_month,
    kpi_top_products,
    kpi_region_stats,
    detect_outliers_zscore,
)

app = typer.Typer(add_completion=False)


@app.command()
def etl(files: list[str], out: str = "data/processed/sales.parquet"):
    df = read_many(files)
    path = write_parquet(df, out)
    typer.echo(f"ok -> {path}")


@app.command()
def kpis(parquet: str):
    df = pd.read_parquet(parquet)
    res = {
        "by_month": kpi_sales_by_month(df).to_dict(orient="records"),
        "top_products": kpi_top_products(df).to_dict(orient="records"),
        "regions": kpi_region_stats(df).to_dict(orient="records"),
        "outliers": detect_outliers_zscore(df)[["date", "product", "revenue"]].to_dict(
            orient="records"
        ),
    }
    typer.echo(json.dumps(res, indent=2, default=str))


@app.command()
def gen_sample(out: str = "data/raw/sales_data.csv", n: int = 500):
    import pandas as pd

    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", "2025-01-01", freq="D")
    picks = rng.choice(dates, size=n)
    date_str = pd.to_datetime(picks).strftime("%Y-%m-%d")

    products = ["Widget", "Gadget", "Thing", "Tool", "Device"]
    regions = ["NA", "EU", "APAC", "LATAM"]
    customers = [f"cust{i:03d}" for i in range(1, 51)]

    df = pd.DataFrame(
        {
            "date": date_str,
            "product": rng.choice(products, size=n),
            "price": rng.integers(5, 100, size=n).astype(float),
            "quantity": rng.integers(1, 20, size=n),
            "customer": rng.choice(customers, size=n),
            "region": rng.choice(regions, size=n),
        }
    )
    df["revenue"] = df["price"] * df["quantity"]

    Path(out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"ok -> {out} ({len(df)} rows)")


if __name__ == "__main__":
    app()
