import pandas as pd

CORE = ["date", "product", "price", "quantity"]
SYN = {
    "fecha": "date",
    "order_date": "date",
    "producto": "product",
    "item": "product",
    "precio": "price",
    "qty": "quantity",
    "cantidad": "quantity",
    "cliente": "customer",
    "region_name": "region",
}


def _canon(c: str) -> str:
    return c.strip().lower().replace(" ", "_").replace("-", "_")


def basic_schema(df: pd.DataFrame) -> None:
    df.columns = [SYN.get(_canon(c), _canon(c)) for c in df.columns]
    missing = [c for c in CORE if c not in df.columns]
    if missing:
        raise ValueError(f"faltan columnas: {missing}")


def normalize_types(df):

    df = df.copy()

    for c in ("date", "product", "price", "quantity", "customer", "region"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    df["date"] = pd.to_datetime(
        df["date"].astype(str).str.slice(0, 10),
        format="%Y-%m-%d",
        errors="coerce",
        utc=True,
    )

    df["price"] = pd.to_numeric(
        df["price"].astype(str).str.replace(",", ".", regex=False), errors="coerce"
    )
    df["quantity"] = pd.to_numeric(df["quantity"].astype(str), errors="coerce")

    df["revenue"] = (df["price"] * df["quantity"]).astype("float64")
    return df


def drop_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    m_date = df["date"].notna()
    m_price = df["price"].notna()
    m_qty = df["quantity"].notna()
    m_price_ok = df["price"].ge(0)
    m_qty_ok = df["quantity"].gt(0)
    print(
        {
            "rows": before,
            "date_ok": int(m_date.sum()),
            "price_ok": int(m_price.sum()),
            "qty_ok": int(m_qty.sum()),
        }
    )
    df = df[m_date & m_price & m_qty & m_price_ok & m_qty_ok]
    return df.reset_index(drop=True)
