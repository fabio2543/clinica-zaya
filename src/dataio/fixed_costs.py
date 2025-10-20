# --- novas utilidades ---
from pathlib import Path
import pandas as pd
from src.utils.helpers import (
    generate_uuid,
    generate_record_key,
    normalize_text,
    utc_now,
)


# Caminhos principais de armazenamento
DATA = Path("data")
SILVER = DATA / "silver"
DIM_DIR = SILVER
FACT_DIR = SILVER / "fact_fixed_cost"

# Sugestões iniciais (semente)
SEED_DIM = {
    "category": [
        "Aluguel",
        "Energia",
        "Água",
        "Internet",
        "Telefone",
        "Limpeza",
        "Segurança",
        "Contabilidade",
        "Marketing",
        "Softwares",
        "Manutenção",
        "Impostos",
        "Folha de pagamento",
    ],
    "payment_method": ["Pix", "Boleto", "Cartão", "TED", "Dinheiro"],
    "cost_center": [
        "Administrativo",
        "Recepção",
        "Sala 1",
        "Sala 2",
        "Comercial",
        "Financeiro",
    ],
    "vendor": ["Imobiliária XYZ", "Concessionária de Energia", "Operadora de Internet"],
}


def ensure_seed_dimensions():
    """Garante que as sugestões existam nas dimensões (sem duplicar)."""
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


def _part_file_for_period(period: str) -> Path:
    year = int(str(period)[:4])
    month = int(str(period)[5:7])
    part_dir = FACT_DIR / f"year={year}" / f"month={month}"
    part_dir.mkdir(parents=True, exist_ok=True)
    return part_dir / "data.parquet"


def _ensure_df(path: Path, cols: list[str]) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame(columns=cols)


def list_categories() -> list[str]:
    p = DIM_DIR / "dim_category.parquet"
    if not p.exists():
        return []
    df = pd.read_parquet(p)
    # ✅ Apenas registros ativos
    df = df[df.get("is_active", True)]
    return (
        df["category_name"]
        .dropna()
        .map(str)
        .map(str.strip)
        .str.capitalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


def list_vendors() -> list[str]:
    p = DIM_DIR / "dim_vendor.parquet"
    if not p.exists():
        return []
    df = pd.read_parquet(p)
    df = df[df.get("is_active", True)]  # ✅
    return (
        df["vendor_name"]
        .dropna()
        .map(str)
        .map(str.strip)
        .str.capitalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


def list_cost_centers() -> list[str]:
    p = DIM_DIR / "dim_cost_center.parquet"
    if not p.exists():
        return []
    df = pd.read_parquet(p)
    df = df[df.get("is_active", True)]  # ✅
    return (
        df["cost_center_name"]
        .dropna()
        .map(str)
        .map(str.strip)
        .str.capitalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


def list_payment_methods() -> list[str]:
    p = DIM_DIR / "dim_payment_method.parquet"
    if not p.exists():
        return []
    df = pd.read_parquet(p)
    df = df[df.get("is_active", True)]  # ✅
    return (
        df["payment_method_name"]
        .dropna()
        .map(str)
        .map(str.strip)
        .str.capitalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


# --- leitura com joins nas dimensões ---


def read_fixed_costs(
    period: str | None = None, category: str | None = None
) -> pd.DataFrame:
    if not FACT_DIR.exists():
        return pd.DataFrame()
    frames = []
    for path in FACT_DIR.glob("year=*/month=*/data.parquet"):
        frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    fact = pd.concat(frames, ignore_index=True)
    fact = fact[~fact["is_deleted"]] if "is_deleted" in fact.columns else fact

    # carregar dims
    def _dim(name):
        p = DIM_DIR / f"dim_{name}.parquet"
        return (
            pd.read_parquet(p)
            if p.exists()
            else pd.DataFrame(columns=[f"{name}_id", f"{name}_code", f"{name}_name"])
        )

    dcat, dven, dcc, dpm = (
        _dim("category"),
        _dim("vendor"),
        _dim("cost_center"),
        _dim("payment_method"),
    )

    fact = fact.merge(
        dcat[["category_id", "category_name"]], on="category_id", how="left"
    )
    fact = fact.merge(dven[["vendor_id", "vendor_name"]], on="vendor_id", how="left")
    fact = fact.merge(
        dcc[["cost_center_id", "cost_center_name"]], on="cost_center_id", how="left"
    )
    fact = fact.merge(
        dpm[["payment_method_id", "payment_method_name"]],
        on="payment_method_id",
        how="left",
    )

    if period:
        fact = fact[fact["period"] == period]
    if category:
        fact = fact[fact["category_name"] == category]

    # colunas amigáveis para o app
    cols = [
        "fixed_cost_id",
        "period",
        "date",
        "description",
        "category_name",
        "amount",
        "payment_method_name",
        "vendor_name",
        "recurrence",
        "due_day",
        "cost_center_name",
        "invoice_number",
        "notes",
        "version",
    ]
    return fact.reindex(columns=[c for c in cols if c in fact.columns]).sort_values(
        ["period", "date"], ascending=[False, False]
    )


# --- create / update / delete unitário ---


def create_fixed_cost(row: dict) -> dict:
    df = pd.DataFrame([row])
    return upsert_fixed_costs(df)


def update_fixed_cost(fixed_cost_id: str, updates: dict) -> bool:
    # abre pela partição do period do registro
    for path in FACT_DIR.glob("year=*/month=*/data.parquet"):
        fact = pd.read_parquet(path)
        if fact.empty:
            continue
        mask = fact["fixed_cost_id"] == fixed_cost_id
        if mask.any():
            for k, v in updates.items():
                if k in fact.columns:
                    fact.loc[mask, k] = v
            fact.loc[mask, "updated_at"] = utc_now()
            fact.loc[mask, "version"] = fact.loc[mask, "version"].fillna(1) + 1
            fact.to_parquet(path, index=False)
            return True
    return False


def delete_fixed_cost(ids: list[str]) -> int:
    """
    Exclusão FÍSICA: remove as linhas dos arquivos Parquet.
    """
    count = 0
    for path in FACT_DIR.glob("year=*/month=*/data.parquet"):
        fact = pd.read_parquet(path) if path.exists() else pd.DataFrame()
        if fact.empty:
            continue
        before = len(fact)
        fact = fact[~fact["fixed_cost_id"].isin(ids)].reset_index(drop=True)
        removed = before - len(fact)
        if removed > 0:
            fact.to_parquet(path, index=False)
            count += removed
    return count


def get_or_create_dim_value(dim_name: str, value: str):
    """
    Cria ou recupera o ID da dimensão (category, vendor, cost_center, payment_method)
    """

    id_col = f"{dim_name}_id"
    code_col = f"{dim_name}_code"
    path = DIM_DIR / f"dim_{dim_name}.parquet"

    # Cria DataFrame vazio caso o arquivo ainda não exista
    if path.exists():
        df = pd.read_parquet(path)
    else:
        df = pd.DataFrame(
            columns=[
                id_col,
                code_col,
                f"{dim_name}_name",
                "created_at",
                "updated_at",
                "is_active",
            ]
        )

    # Valor padrão
    if not value or str(value).strip() == "":
        value = "Não informado"

    # Normaliza o código
    code = normalize_text(value)

    # Verifica se já existe
    row = df[df[code_col] == code]
    if not row.empty:
        return int(row.iloc[0][id_col])

    # Cria novo ID
    new_id = (df[id_col].max() + 1) if not df.empty else 1
    new_row = {
        id_col: int(new_id),
        code_col: code,
        f"{dim_name}_name": value,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "is_active": True,
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return int(new_id)


def upsert_fixed_costs(df: pd.DataFrame) -> dict:
    """
    Recebe DataFrame normalizado e aplica insert/update (upsert) em fact_fixed_cost.

    Campos esperados (case-insensitive):
      - period* (AAAA-MM)
      - date* (YYYY-MM-DD)
      - description*
      - category* (texto) -> dim_category
      - amount* (float)
      - payment_method (texto) -> dim_payment_method
      - vendor (texto) -> dim_vendor
      - cost_center (texto) -> dim_cost_center
      - recurrence (texto opcional)
      - due_day (int opcional)
      - invoice_number (texto opcional)
      - notes (texto opcional)
    """
    results = {"inserted": 0, "updated": 0, "errors": 0}

    if df is None or df.empty:
        return results

    # --- Normalização de colunas ---
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Campos mínimos
    required = ["period", "date", "description", "category", "amount"]
    for c in required:
        if c not in df.columns:
            df[c] = ""

    # Coerções seguras
    # amount
    df["amount"] = (
        pd.to_numeric(df["amount"], errors="coerce").fillna(0.0).astype(float)
    )
    # date
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
    # period (AAAA-MM): vamos validar string e extrair ano/mês
    df["period"] = df["period"].astype(str).str.strip()

    # due_day
    if "due_day" in df.columns:
        df["due_day"] = pd.to_numeric(df["due_day"], errors="coerce").astype("Int64")
    else:
        df["due_day"] = pd.Series([pd.NA] * len(df), dtype="Int64")

    # Campos texto
    for col in [
        "description",
        "category",
        "payment_method",
        "vendor",
        "cost_center",
        "recurrence",
        "invoice_number",
        "notes",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
        else:
            df[col] = ""

    for _, row in df.iterrows():
        try:
            # Validações mínimas
            if not row["period"] or len(row["period"]) < 7 or row["period"][4] != "-":
                raise ValueError(
                    f"period inválido: '{row['period']}' (esperado AAAA-MM)"
                )
            if not row["description"] or row["amount"] is None:
                raise ValueError("description/amount obrigatórios ausentes")
            if not row["date"] or str(row["date"]).lower() in ["nat", "nan"]:
                raise ValueError("date inválida/ausente")

            # Extrai ano/mês com segurança
            try:
                year = int(row["period"][:4])
                month = int(row["period"][5:7])
            except Exception:
                raise ValueError(
                    f"period inválido: '{row['period']}' (esperado AAAA-MM)"
                )

            part_dir = FACT_DIR / f"year={year}" / f"month={month:02d}"
            part_dir.mkdir(parents=True, exist_ok=True)
            part_file = part_dir / "data.parquet"

            # normaliza date
            date_iso = (
                pd.to_datetime(str(row["date"]), errors="raise").date().isoformat()
            )

            rk = generate_record_key(
                row["period"],
                date_iso,
                row["description"],
                float(row["amount"]),
                row.get("vendor", ""),
            )

            fact = pd.read_parquet(part_file) if part_file.exists() else pd.DataFrame()

            if (
                (not fact.empty)
                and ("record_key" in fact.columns)
                and (rk in fact["record_key"].astype(str).values)
            ):
                # update
                idx = fact.index[fact["record_key"].astype(str) == rk][0]

                def _set(col, val):
                    if col in fact.columns:
                        fact.loc[idx, col] = val

                _set("description", row.get("description"))
                _set("amount", float(row.get("amount", 0) or 0))
                _set("invoice_number", row.get("invoice_number"))
                _set("notes", row.get("notes"))
                # dimensões
                if str(row.get("category", "")).strip():
                    _set(
                        "category_id",
                        get_or_create_dim_value("category", row.get("category")),
                    )
                if str(row.get("vendor", "")).strip():
                    _set(
                        "vendor_id",
                        get_or_create_dim_value("vendor", row.get("vendor")),
                    )
                if str(row.get("cost_center", "")).strip():
                    _set(
                        "cost_center_id",
                        get_or_create_dim_value("cost_center", row.get("cost_center")),
                    )
                if str(row.get("payment_method", "")).strip():
                    _set(
                        "payment_method_id",
                        get_or_create_dim_value(
                            "payment_method", row.get("payment_method")
                        ),
                    )
                _set("recurrence", row.get("recurrence"))
                if (
                    row.get("due_day") is not pd.NA
                    and row.get("due_day") is not None
                    and str(row.get("due_day")) != ""
                ):
                    try:
                        _set("due_day", int(row.get("due_day")))
                    except Exception:
                        pass
                fact.loc[idx, "updated_at"] = utc_now()
                if "version" in fact.columns:
                    try:
                        fact.loc[idx, "version"] = (
                            fact.loc[idx, "version"].fillna(1)
                            if hasattr(fact.loc[idx, "version"], "fillna")
                            else fact.loc[idx, "version"]
                        ) + 1
                    except Exception:
                        fact.loc[idx, "version"] = 2
                else:
                    fact.loc[idx, "version"] = 2
                results["updated"] += 1
            else:
                # insert
                new_row = {
                    "fixed_cost_id": generate_uuid(),
                    "record_key": rk,
                    "year": year,
                    "month": month,
                    "period": row["period"],
                    "date": date_iso,
                    "description": row["description"],
                    "category_id": get_or_create_dim_value(
                        "category", row.get("category")
                    ),
                    "amount": float(row["amount"]),
                    "payment_method_id": get_or_create_dim_value(
                        "payment_method", row.get("payment_method")
                    ),
                    "vendor_id": get_or_create_dim_value("vendor", row.get("vendor")),
                    "recurrence": row.get("recurrence"),
                    "due_day": (
                        int(row["due_day"]) if pd.notna(row["due_day"]) else None
                    ),
                    "cost_center_id": get_or_create_dim_value(
                        "cost_center", row.get("cost_center")
                    ),
                    "invoice_number": row.get("invoice_number"),
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
