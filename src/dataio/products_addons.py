"""
Helpers adicionais para o módulo de produtos.
Se você já tiver essas funções no seu data layer, pode ignorar este arquivo.
"""

from pathlib import Path
import pandas as pd

DIM_CATEGORY_FILE = Path("data/silver/dim_category.parquet")
FACT_PRODUCT_FILE = Path("data/silver/fact_product/products.parquet")


def list_categories() -> list[str]:
    if DIM_CATEGORY_FILE.exists():
        df = pd.read_parquet(DIM_CATEGORY_FILE)
        if not df.empty and "category" in df.columns:
            return sorted(df["category"].dropna().astype(str).unique().tolist())
    if FACT_PRODUCT_FILE.exists():
        df = pd.read_parquet(FACT_PRODUCT_FILE)
        if not df.empty and "category" in df.columns:
            return sorted(df["category"].dropna().astype(str).unique().tolist())
    return ["produto"]
