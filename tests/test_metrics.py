import pandas as pd
from sales_etl.metrics import kpi_sales_by_month, detect_outliers_zscore


def _df():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-02-01"]),
            "product": ["A", "A", "B"],
            "price": [10, 10, 20],
            "quantity": [1, 100, 1],
            "customer": ["c1", "c2", "c3"],
            "region": ["NA", "NA", "EU"],
            "revenue": [10, 1000, 20],
            "date_month": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-02-01"]),
        }
    )


def test_by_month():
    df = _df()
    m = kpi_sales_by_month(df)
    assert m["revenue_total"].sum() == 1030


def test_outliers():
    df = _df()
    out = detect_outliers_zscore(df, z=2.0)
    assert not out.empty
