import numpy as np
import pandas as pd


def kpi_sales_by_month(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["revenue"])

    if df is None or df.empty:
        return pd.DataFrame(columns=["date_month", "revenue_total"])
    g = df.groupby("date_month")["revenue"].sum().reset_index(name="revenue_total")
    return g.sort_values("date_month")


def kpi_top_products(df: pd.DataFrame, k: int = 10) -> pd.DataFrame:
    df = df.copy()
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["revenue"])

    if df is None or df.empty:
        return pd.DataFrame(columns=["product", "revenue_total"])
    g = df.groupby("product")["revenue"].sum().reset_index(name="revenue_total")
    return g.sort_values("revenue_total", ascending=False).head(k)


def kpi_region_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["revenue"])

    if df is None or df.empty:
        return pd.DataFrame(
            columns=["region", "revenue_total", "revenue_mean", "revenue_std", "orders"]
        )
    df = df.copy()
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df = df.dropna(subset=["revenue"])
    base = (
        df.groupby("region")["revenue"]
        .agg(
            revenue_total="sum",
            revenue_mean="mean",
            orders="count",
        )
        .reset_index()
    )
    std = (
        df.groupby("region")["revenue"]
        .apply(lambda s: float(np.std(s, ddof=1)) if len(s) > 1 else 0.0)
        .reset_index(name="revenue_std")
    )
    return base.merge(std, on="region", how="left").sort_values(
        "revenue_total", ascending=False
    )


def detect_outliers_zscore(
    df: pd.DataFrame, z: float = 3.0, method: str = "auto"
) -> pd.DataFrame:
    if df is None or df.empty:
        return df.iloc[0:0]

    x = pd.to_numeric(df["revenue"], errors="coerce").to_numpy(dtype="float64")
    valid = np.isfinite(x)
    if not valid.any():
        return df.iloc[0:0]
    x = x[valid]
    idx = np.where(valid)[0]

    use_mad = (method == "mad") or (method == "auto" and x.size < 25)
    if use_mad:
        med = np.median(x)
        mad = np.median(np.abs(x - med))
        if mad == 0:
            return df.iloc[0:0]
        mz = 0.6745 * (x - med) / mad
        sel = np.abs(mz) >= z
    else:
        mu = x.mean()
        sigma = x.std(ddof=0)
        if not np.isfinite(sigma) or sigma == 0:
            return df.iloc[0:0]
        sel = np.abs((x - mu) / sigma) >= z

    return df.iloc[idx[sel]]
