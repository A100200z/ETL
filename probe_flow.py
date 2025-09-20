from sales_etl.etl import _read_one
from sales_etl.validators import normalize_types, drop_bad_rows

csv_path = "data/raw/sales_data.csv"
df = _read_one(csv_path)
print("raw dtypes:", df.dtypes.to_dict())
print(df.head().to_string())

n = normalize_types(df)
print({"date_NaT": int(n["date"].isna().sum()),
       "price_NaN": int(n["price"].isna().sum()),
       "qty_NaN": int(n["quantity"].isna().sum())})
f = drop_bad_rows(n)
print("after_drop:", f.shape)
print(f.head().to_string())
