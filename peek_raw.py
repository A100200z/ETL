import pandas as pd

df = pd.read_csv(
    "data/raw/sales_data.csv",
    sep=None,
    engine="python",
    encoding="utf-8-sig",
    dtype=str,
    keep_default_na=False,
)
print("cols:", list(df.columns))
print("date head repr:", [repr(x) for x in df["date"].head(5).tolist()])
print("price head repr:", [repr(x) for x in df["price"].head(5).tolist()])
print("qty head repr:", [repr(x) for x in df["quantity"].head(5).tolist()])
