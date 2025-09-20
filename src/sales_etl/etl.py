# src/sales_etl/etl.py
import pandas as pd
from pathlib import Path
from .validators import basic_schema, normalize_types, drop_bad_rows


def _read_one(p):
    import re

    # intento normal
    try:
        df = pd.read_csv(
            p,
            sep=None,
            engine="python",
            encoding="utf-8-sig",
            dtype=str,
            keep_default_na=False,
        )
        # si ya viene bien, úsalo
        cols = {c.strip().lower() for c in df.columns}
        if {"date", "product", "price", "quantity"}.issubset(cols) and df.get(
            "price"
        ) is not None:
            return df
    except Exception:
        pass

    # Fallback: línea “impresa” tipo:
    # 0  2024-02-02   Widget   81.0   11  cust044  APAC  891.0
    raw = pd.read_csv(p, header=None, names=["line"], dtype=str, encoding="utf-8-sig")
    pat = re.compile(
        r"^\s*\d+\s+(\d{4}-\d{2}-\d{2})\s+([A-Za-z]+)\s+(\d+(?:\.\d+)?)\s+(\d+)\s+(cust\d+)\s+([A-Za-z]+)\s+(\d+(?:\.\d+)?)\s*$"
    )
    rows = []
    for s in raw["line"].dropna():
        m = pat.match(s.strip())
        if m:
            rows.append(m.groups())
    if not rows:
        raise ValueError(
            "No pude parsear el archivo. Sube un CSV real o ajusta el regex."
        )
    df = pd.DataFrame(
        rows,
        columns=[
            "date",
            "product",
            "price",
            "quantity",
            "customer",
            "region",
            "revenue",
        ],
    )
    return df


def read_many(paths: list[str]) -> pd.DataFrame:
    frames = [_read_one(p) for p in paths]
    df = pd.concat(frames, ignore_index=True)
    basic_schema(df)
    df = normalize_types(df)
    df = drop_bad_rows(df)
    date = df["date"]
    try:
        date = date.dt.tz_convert(None)
    except:
        pass
    df["date_month"] = date.dt.to_period("M").dt.to_timestamp()
    return df


def write_parquet(df: pd.DataFrame, out_path: str | Path) -> str:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return str(out)
