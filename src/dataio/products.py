from pathlib import Path
import pandas as pd
from src.utils.helpers import (
    generate_uuid,
    normalize_text,
    utc_now,
)

# Caminhos principais
DATA = Path("data")
SILVER = DATA / "silver"
DIM_DIR = SILVER
FACT_DIR = SILVER / "fact_product_purchase"  # partição por data da compra

# Sementes (inclui "Produto" como categoria padrão)
SEED_DIM = {
    "category": [
        "Produto",
        "Insumos",
        "Injetáveis",
        "Equipamentos",
        "Descartáveis",
        "Outros",
    ],
    "vendor": ["Fornecedor Geral", "Distribuidora XYZ"],
}


def get_or_create_dim_value(dim_name: str, value: str | None):
    id_col = f"{dim_name}_id"
    code_col = f"{dim_name}_code"
    name_col = f"{dim_name}_name"
    path = DIM_DIR / f"dim_{dim_name}.parquet"

    if path.exists():
        df = pd.read_parquet(path)
    else:
        df = pd.DataFrame(
            columns=[
                id_col,
                code_col,
                name_col,
                "created_at",
                "updated_at",
                "is_active",
            ]
        )

    if not value or str(value).strip() == "":
        value = "Não informado"

    code = normalize_text(value)
    row = df[df[code_col] == code]
    if not row.empty:
        return int(row.iloc[0][id_col])

    new_id = (df[id_col].max() + 1) if not df.empty else 1
    new_row = {
        id_col: int(new_id),
        code_col: code,
        name_col: value,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "is_active": True,
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return int(new_id)


def ensure_seed_dimensions():
    for dim_name, items in SEED_DIM.items():
        for val in items:
            try:
                get_or_create_dim_value(dim_name, val)
            except Exception:
                pass


try:
    ensure_seed_dimensions()
except Exception:
    pass


def _part_file_for_date(purchase_date: str) -> Path:
    dt = pd.to_datetime(purchase_date).date()
    year = dt.year
    month = dt.month
    part_dir = FACT_DIR / f"year={year}" / f"month={month:02d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    return part_dir / "data.parquet"


def _ensure_df(path: Path, cols: list[str]) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=cols)


def list_vendors() -> list[str]:
    p = DIM_DIR / "dim_vendor.parquet"
    if not p.exists():
        return []
    df = pd.read_parquet(p)
    df = df[df.get("is_active", True)]
    return (
        df["vendor_name"]
        .dropna()
        .map(str)
        .map(str.strip)
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


# ----------------- READ (com joins em dimensões) -----------------
def read_products(category: str | None = None) -> pd.DataFrame:
    if not FACT_DIR.exists():
        return pd.DataFrame()

    frames = []
    for path in FACT_DIR.glob("year=*/month=*/data.parquet"):
        frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()

    fact = pd.concat(frames, ignore_index=True)

    def _dim(name):
        p = DIM_DIR / f"dim_{name}.parquet"
        return (
            pd.read_parquet(p)
            if p.exists()
            else pd.DataFrame(columns=[f"{name}_id", f"{name}_code", f"{name}_name"])
        )

    dcat = _dim("category")
    dven = _dim("vendor")

    if "category_id" in fact.columns and not dcat.empty:
        fact = fact.merge(
            dcat[["category_id", "category_name"]], on="category_id", how="left"
        )
    if "vendor_id" in fact.columns and not dven.empty:
        fact = fact.merge(
            dven[["vendor_id", "vendor_name"]], on="vendor_id", how="left"
        )

    if category:
        if "category_name" in fact.columns:
            fact = fact[fact["category_name"] == category]

    cols = [
        "product_purchase_id",
        "purchase_date",
        "product_name",
        "sku",
        "unit_price",
        "quantity",
        "category_name",
        "vendor_name",
        "notes",
        "version",
    ]
    cols = [c for c in cols if c in fact.columns]
    if not cols:
        return fact
    return fact.reindex(columns=cols).sort_values(
        ["purchase_date", "product_name"], ascending=[False, True]
    )


# ----------------- CREATE / UPDATE / DELETE -----------------
def create_product_purchase(row: dict) -> dict:
    df = pd.DataFrame([row])
    return upsert_products(df)


def update_product_purchase(product_purchase_id: str, updates: dict) -> bool:
    for path in FACT_DIR.glob("year=*/month=*/data.parquet"):
        fact = pd.read_parquet(path)
        if fact.empty:
            continue
        mask = fact["product_purchase_id"] == product_purchase_id
        if mask.any():
            for k, v in updates.items():
                if k in fact.columns:
                    fact.loc[mask, k] = v
            fact.loc[mask, "updated_at"] = utc_now()
            fact.loc[mask, "version"] = (
                fact.loc[mask, "version"].fillna(1) + 1
                if "version" in fact.columns
                else 2
            )
            fact.to_parquet(path, index=False)
            return True
    return False


def delete_product_purchase(ids: list[str]) -> int:
    """
    Exclusão FÍSICA: remove as linhas dos arquivos Parquet.
    """
    count = 0
    for path in FACT_DIR.glob("year=*/month=*/data.parquet"):
        fact = pd.read_parquet(path) if path.exists() else pd.DataFrame()
        if fact.empty:
            continue
        before = len(fact)
        fact = fact[~fact["product_purchase_id"].isin(ids)].reset_index(drop=True)
        removed = before - len(fact)
        if removed > 0:
            fact.to_parquet(path, index=False)
            count += removed
    return count


# ----------------- UPSERT (Carga unitária/massiva) -----------------
def upsert_products(df: pd.DataFrame) -> dict:
    """
    Espera colunas (normalizado):
      - product_name*, unit_price*, purchase_date*, quantity*
      - sku, category, vendor, notes
    Usa record_key para idempotência por (purchase_date, product_name, unit_price, quantity, vendor, sku).
    """

    # --- Coerções seguras ---
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Coagir numéricos
    if "unit_price" in df.columns:
        df["unit_price"] = (
            pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0).astype(float)
        )
    else:
        df["unit_price"] = 0.0

    if "quantity" in df.columns:
        df["quantity"] = (
            pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        )
    else:
        df["quantity"] = 0

    # Datas
    if "purchase_date" in df.columns:
        df["purchase_date"] = pd.to_datetime(
            df["purchase_date"], errors="coerce"
        ).dt.date.astype("string")
    else:
        df["purchase_date"] = pd.NaT

    # Strings
    for col in ["product_name", "sku", "category", "vendor", "notes"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
        else:
            df[col] = ""

    # Categoria padrão = "Produto"
    if "category" in df.columns:
        df["category"] = df["category"].replace({"": "Produto"}).fillna("Produto")
    else:
        df["category"] = "Produto"

    results = {"inserted": 0, "updated": 0, "errors": 0}

    for _, row in df.iterrows():
        try:
            # valida purchase_date
            if not row.get("purchase_date") or str(row["purchase_date"]).lower() in [
                "nat",
                "nan",
            ]:
                raise ValueError("purchase_date inválida/ausente")

            purchase_date = (
                pd.to_datetime(str(row["purchase_date"]), errors="raise")
                .date()
                .isoformat()
            )
            part_file = _part_file_for_date(purchase_date)

            # Chave idempotente (sem usar generate_record_key para evitar aridade)
            rk = normalize_text(
                f"{purchase_date}|{row.get('product_name', '')}|"
                f"{float(row.get('unit_price', 0) or 0):.4f}|"
                f"{int(row.get('quantity', 0) or 0)}|"
                f"{row.get('vendor', '')}|{row.get('sku', '')}"
            )

            fact = pd.read_parquet(part_file) if part_file.exists() else pd.DataFrame()

            if (
                (not fact.empty)
                and ("record_key" in fact.columns)
                and (rk in fact["record_key"].values)
            ):
                # update
                idx = fact.index[fact["record_key"] == rk][0]
                for col in ["product_name", "sku", "unit_price", "quantity", "notes"]:
                    if col in fact.columns:
                        fact.loc[idx, col] = row.get(col, fact.loc[idx, col])
                # atualiza dimensões se vierem
                if "category" in row and str(row["category"]).strip() != "":
                    fact.loc[idx, "category_id"] = get_or_create_dim_value(
                        "category", row.get("category")
                    )
                if "vendor" in row and str(row["vendor"]).strip() != "":
                    fact.loc[idx, "vendor_id"] = get_or_create_dim_value(
                        "vendor", row.get("vendor")
                    )
                fact.loc[idx, "updated_at"] = utc_now()
                current_version = (
                    fact.loc[idx, "version"] if "version" in fact.columns else 1
                )
                try:
                    fact.loc[idx, "version"] = (
                        int(current_version) if pd.notna(current_version) else 1
                    ) + 1
                except Exception:
                    fact.loc[idx, "version"] = 2
                results["updated"] += 1
            else:
                # insert
                dt = pd.to_datetime(purchase_date)
                new_row = {
                    "product_purchase_id": generate_uuid(),
                    "record_key": rk,
                    "year": dt.year,
                    "month": dt.month,
                    "purchase_date": purchase_date,
                    "product_name": row.get("product_name"),
                    "sku": row.get("sku"),
                    "unit_price": float(row.get("unit_price", 0) or 0),
                    "quantity": int(row.get("quantity", 0) or 0),
                    "category_id": get_or_create_dim_value(
                        "category", row.get("category")
                    ),
                    "vendor_id": get_or_create_dim_value("vendor", row.get("vendor")),
                    "notes": row.get("notes"),
                    "created_at": utc_now(),
                    "updated_at": utc_now(),
                    "is_deleted": False,
                    "version": 1,
                }
                fact = pd.concat([fact, pd.DataFrame([new_row])], ignore_index=True)
                results["inserted"] += 1

            fact.to_parquet(part_file, index=False)
        except Exception as e:
            print(f"Erro ao processar linha: {e}")
            results["errors"] += 1

    return results


DIM_CATEGORY_FILE = Path("data/silver/dim_category.parquet")
FACT_PRODUCT_FILE = Path("data/silver/fact_product/products.parquet")


def list_categories() -> list[str]:
    """
    Retorna a lista de categorias disponíveis.
    1) Tenta dim_category.parquet
    2) Cai para fact_product/products.parquet
    3) Fallback: ['produto']
    """
    if DIM_CATEGORY_FILE.exists():
        df = pd.read_parquet(DIM_CATEGORY_FILE)
        if not df.empty and "category" in df.columns:
            return sorted(df["category"].dropna().astype(str).unique().tolist())

    if FACT_PRODUCT_FILE.exists():
        df = pd.read_parquet(FACT_PRODUCT_FILE)
        if not df.empty and "category" in df.columns:
            return sorted(df["category"].dropna().astype(str).unique().tolist())

    return ["produto"]  # padrão do projeto
