"""
Streamlit – Módulo Procedimentos | Zaya

Funções principais:
- Catálogo de Procedimentos (CRUD com BOM e Comissão)
- Registro de Vendas/Execuções
- Preview dinâmico de margem (com desconto, gateway fee e overhead)

Persistência em Parquet (camada silver) com versionamento simples e soft delete.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# ============================
# CONFIG & PATHS
# ============================
DATA_DIR = Path("data/silver")
CATALOG_PATH = DATA_DIR / "fact_procedure"
SALE_PATH = DATA_DIR / "fact_procedure_sale"
PACKAGE_PATH = DATA_DIR / "fact_procedure_package"

for p in [CATALOG_PATH, SALE_PATH, PACKAGE_PATH]:
    p.mkdir(parents=True, exist_ok=True)

CATALOG_FILE = CATALOG_PATH / "catalog.parquet"
SALE_FILE = SALE_PATH / "sales.parquet"

# ============================
# HELPERS
# ============================

def utc_now() -> str:
    return datetime.utcnow().isoformat()


def generate_uuid() -> str:
    # UUID-lite: sha256 de now + rand
    raw = f"{datetime.utcnow().timestamp()}-{np.random.rand()}".encode()
    return hashlib.sha256(raw).hexdigest()[:32]


def record_key_from_dict(d: Dict) -> str:
    payload = json.dumps(d, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def save_parquet(df: pd.DataFrame, path: Path) -> None:
    if df is None:
        return
    # Garantir tipos básicos
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


# ============================
# DOMAIN – CÁLCULOS
# ============================

def calc_base_cost(bom_rows: List[Dict]) -> float:
    if not bom_rows:
        return 0.0
    total = 0.0
    for r in bom_rows:
        qty = float(r.get("qty", 0) or 0)
        unit_cost = float(r.get("unit_cost", 0) or 0)
        total += qty * unit_cost
    return float(round(total, 2))


def apply_commission(sale_price: float,
                     model: str,
                     rate: float | None = None,
                     fixed_value: float | None = None,
                     tiers_json: str | None = None) -> Tuple[float, str, float, Optional[str]]:
    model = (model or "percent").lower()
    tiers_used = None
    if model == "percent":
        rate = float(rate or 0)
        val = sale_price * rate
        return float(round(val, 2)), model, rate, None
    if model == "fixed":
        val = float(fixed_value or 0)
        return float(round(val, 2)), model, 0.0, None
    if model == "tiered":
        # tiers_json esperado: [{"min":0,"max":500,"rate":0.2},{"min":500,"max":999999,"rate":0.3}]
        try:
            tiers = json.loads(tiers_json or "[]")
        except Exception:
            tiers = []
        applied_rate = 0.0
        for t in tiers:
            mn = float(t.get("min", 0))
            mx = float(t.get("max", 9e18))
            if mn <= sale_price < mx:
                applied_rate = float(t.get("rate", 0))
                tiers_used = json.dumps(t)
                break
        val = sale_price * applied_rate
        return float(round(val, 2)), model, applied_rate, tiers_used
    # fallback
    return 0.0, "percent", 0.0, None


def calc_overhead(overhead_model: str,
                  overhead_rate_value: float,
                  sale_price: float,
                  duration_minutes: float) -> float:
    m = (overhead_model or "none").lower()
    v = float(overhead_rate_value or 0)
    if m == "per_hour":
        return float(round((v * (duration_minutes or 0)) / 60.0, 2))
    if m == "per_session":
        return float(round(v, 2))
    if m == "per_revenue":
        return float(round(sale_price * v, 2))
    return 0.0


def calc_preview(sale_price: float,
                 base_cost: float,
                 commission_model: str,
                 commission_rate: float | None,
                 commission_fixed_value: float | None,
                 commission_tiers: str | None,
                 overhead_model: str,
                 overhead_rate_value: float,
                 duration_minutes: float,
                 gateway_fee_value: float) -> Dict:
    commission_value, model_applied, rate_applied, tier_used = apply_commission(
        sale_price, commission_model, commission_rate, commission_fixed_value, commission_tiers
    )
    overhead_value = calc_overhead(overhead_model, overhead_rate_value, sale_price, duration_minutes)
    net_profit = sale_price - base_cost - commission_value - overhead_value - float(gateway_fee_value or 0)
    margin_percent = (net_profit / sale_price * 100.0) if sale_price > 0 else 0.0
    return {
        "commission_value": round(commission_value, 2),
        "commission_model_applied": model_applied,
        "commission_rate_applied": round(rate_applied, 4),
        "commission_tier_used": tier_used,
        "overhead_value": round(overhead_value, 2),
        "net_profit": round(net_profit, 2),
        "margin_percent": round(margin_percent, 2),
    }


# ============================
# DATA ACCESS – CATÁLOGO
# ============================

def get_catalog() -> pd.DataFrame:
    df = load_parquet(CATALOG_FILE)
    if df.empty:
        cols = [
            "procedure_id", "name", "category", "duration_minutes",
            "price_list", "discount_max_percent",
            "commission_model", "commission_rate", "commission_fixed_value", "commission_tiers",
            "bom_json", "base_cost",
            "overhead_allocation_model", "overhead_rate_value",
            "price_min_recommended", "markup_target_percent",
            "active", "version", "valid_from", "valid_to", "is_deleted",
            "record_key"
        ]
        df = pd.DataFrame(columns=cols)
    return df


def upsert_catalog(row: Dict) -> None:
    df = get_catalog()
    # versionamento simples
    row = row.copy()
    row.setdefault("procedure_id", generate_uuid())
    row.setdefault("version", 1)
    row.setdefault("valid_from", utc_now())
    row.setdefault("valid_to", "")
    row.setdefault("is_deleted", False)
    row.setdefault("active", True)
    # record_key para idempotência (nome + categoria + versão ativa)
    rk_payload = {k: row.get(k) for k in ["name", "category", "active", "version"]}
    row["record_key"] = record_key_from_dict(rk_payload)

    # se já existe procedimento com mesmo name+category ativo, inativar versão anterior
    mask = (df["name"] == row["name"]) & (df["category"] == row["category"]) & (df["active"] == True) & (df["is_deleted"] == False)
    if not df.empty and mask.any():
        df.loc[mask, "active"] = False
        df.loc[mask, "valid_to"] = utc_now()
        df.loc[mask, "version"] = df.loc[mask, "version"].astype(int) + 0

    # append nova versão
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    save_parquet(df, CATALOG_FILE)


def soft_delete_procedure(procedure_id: str) -> None:
    df = get_catalog()
    if df.empty:
        return
    mask = df["procedure_id"] == procedure_id
    df.loc[mask, "is_deleted"] = True
    df.loc[mask, "active"] = False
    df.loc[mask, "valid_to"] = utc_now()
    save_parquet(df, CATALOG_FILE)


# ============================
# DATA ACCESS – VENDAS
# ============================

def get_sales() -> pd.DataFrame:
    df = load_parquet(SALE_FILE)
    if df.empty:
        cols = [
            "procedure_sale_id", "procedure_id", "name", "category", "professional",
            "sale_datetime", "price_list_at_sale", "discount_percent", "sale_price",
            "commission_model_applied", "commission_rate_applied", "commission_value",
            "base_cost_at_sale", "overhead_value", "gateway_fee_value",
            "net_profit", "margin_percent", "source_type", "package_id", "notes"
        ]
        df = pd.DataFrame(columns=cols)
    return df


def add_sale(row: Dict) -> None:
    df = get_sales()
    row = row.copy()
    row.setdefault("procedure_sale_id", generate_uuid())
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    save_parquet(df, SALE_FILE)


# ============================
# UI – STREAMLIT
# ============================

def _bom_editor(initial_rows: Optional[List[Dict]] = None) -> Tuple[List[Dict], float]:
    st.subheader("BOM (Insumos)")
    st.caption("Informe a quantidade e o custo unitário de cada insumo/produto.")
    init = initial_rows or [{"product_code": "", "description": "", "qty": 1.0, "unit_cost": 0.0}]
    df_bom = pd.DataFrame(init)
    edited = st.data_editor(
        df_bom,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "product_code": st.column_config.TextColumn("Código"),
            "description": st.column_config.TextColumn("Descrição"),
            "qty": st.column_config.NumberColumn("Qtd", step=0.1, min_value=0.0),
            "unit_cost": st.column_config.NumberColumn("Custo Unit.", step=0.01, min_value=0.0),
        },
        key="bom_editor",
    )
    rows = edited.replace({np.nan: None}).to_dict(orient="records")
    base_cost = calc_base_cost(rows)
    st.info(f"Custo base (BOM): R$ {base_cost:,.2f}")
    return rows, base_cost


def page_catalog():
    st.header("Catálogo de Procedimentos")
    df = get_catalog()

    with st.expander("Novo / Editar procedimento", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Nome do procedimento")
            category = st.selectbox("Categoria", ["facial", "corporal", "laser", "injetável", "outros"], index=0)
            duration_minutes = st.number_input("Duração (min)", min_value=0, step=5, value=60)
            price_list = st.number_input("Preço de Tabela (R$)", min_value=0.0, step=10.0, value=300.0, format="%0.2f")
            discount_max_percent = st.number_input("Desconto máx. (%)", min_value=0.0, max_value=100.0, step=1.0, value=10.0)
            markup_target_percent = st.number_input("Markup alvo (%) (opcional)", min_value=0.0, step=1.0, value=0.0)
            overhead_allocation_model = st.selectbox("Overhead (opcional)", ["none", "per_hour", "per_session", "per_revenue"], index=0)
            overhead_rate_value = st.number_input("Valor do overhead (depende do modelo)", min_value=0.0, step=1.0, value=0.0)
        with col2:
            commission_model = st.selectbox("Modelo de comissão", ["percent", "fixed", "tiered"], index=0)
            commission_rate = st.number_input("% Comissão (0-1)", min_value=0.0, max_value=1.0, step=0.01, value=0.30)
            commission_fixed_value = st.number_input("Comissão fixa (R$)", min_value=0.0, step=1.0, value=0.0)
            commission_tiers = st.text_area("Tiers (JSON)", placeholder='Ex.: [{"min":0,"max":500,"rate":0.2},{"min":500,"max":999999,"rate":0.3}]')
            active = st.checkbox("Ativo", value=True)

        bom_rows, base_cost = _bom_editor()

        # Preview simples usando preço de tabela e gateway/overhead
        st.subheader("Preview de Margem (Preço de Tabela)")
        gateway_fee_value = st.number_input("Gateway (R$)", min_value=0.0, step=1.0, value=0.0)
        pv = calc_preview(
            sale_price=price_list,
            base_cost=base_cost,
            commission_model=commission_model,
            commission_rate=commission_rate,
            commission_fixed_value=commission_fixed_value,
            commission_tiers=commission_tiers,
            overhead_model=overhead_allocation_model,
            overhead_rate_value=overhead_rate_value,
            duration_minutes=duration_minutes,
            gateway_fee_value=gateway_fee_value,
        )
        st.metric("Margem (%)", f"{pv['margin_percent']}%", help=f"Lucro líquido: R$ {pv['net_profit']:.2f}")
        st.caption(f"Comissão: R$ {pv['commission_value']:.2f} · Overhead: R$ {pv['overhead_value']:.2f}")

        # Preço mínimo recomendado por margem alvo (opcional): aqui usamos markup alvo como referência simples
        price_min_recommended = 0.0
        if markup_target_percent and markup_target_percent > 0:
            k = 1.0 + (markup_target_percent / 100.0)
            # Quando comissão for %: preço * rate -> fica no denominador numa versão refinada.
            # Para simplificar no catálogo: apenas garantir ganho sobre custo direto + overhead + gateway
            price_min_recommended = round((base_cost + pv["overhead_value"] + gateway_fee_value) * k + (commission_fixed_value or 0.0), 2)
            st.write(f"Preço mínimo recomendado (markup alvo): R$ {price_min_recommended:,.2f}")

        if st.button("Salvar/Versionar procedimento", use_container_width=True, type="primary"):
            if not name:
                st.error("Informe o nome do procedimento.")
            else:
                row = {
                    "procedure_id": generate_uuid(),
                    "name": name.strip(),
                    "category": category,
                    "duration_minutes": int(duration_minutes or 0),
                    "price_list": float(price_list or 0),
                    "discount_max_percent": float(discount_max_percent or 0),
                    "commission_model": commission_model,
                    "commission_rate": float(commission_rate or 0),
                    "commission_fixed_value": float(commission_fixed_value or 0),
                    "commission_tiers": commission_tiers.strip(),
                    "bom_json": json.dumps(bom_rows, ensure_ascii=False),
                    "base_cost": float(base_cost or 0),
                    "overhead_allocation_model": overhead_allocation_model,
                    "overhead_rate_value": float(overhead_rate_value or 0),
                    "price_min_recommended": float(price_min_recommended or 0),
                    "markup_target_percent": float(markup_target_percent or 0),
                    "active": bool(active),
                }
                upsert_catalog(row)
                st.success("Procedimento salvo e versionado com sucesso.")
                st.experimental_rerun()

    st.divider()
    st.subheader("Procedimentos cadastrados")
    if df.empty:
        st.info("Nenhum procedimento cadastrado ainda.")
        return

    # Filtros básicos
    c1, c2, c3 = st.columns(3)
    with c1:
        f_cat = st.selectbox("Filtrar por categoria", ["(todas)"] + sorted(df["category"].dropna().unique().tolist()))
    with c2:
        f_status = st.selectbox("Status", ["ativos", "inativos", "todos"], index=0)
    with c3:
        f_text = st.text_input("Busca por nome")

    filtered = df.copy()
    if f_cat != "(todas)":
        filtered = filtered[filtered["category"] == f_cat]
    if f_status == "ativos":
        filtered = filtered[(filtered["active"] == True) & (filtered["is_deleted"] == False)]
    elif f_status == "inativos":
        filtered = filtered[(filtered["active"] == False) & (filtered["is_deleted"] == False)]
    if f_text:
        filtered = filtered[filtered["name"].str.contains(f_text, case=False, na=False)]

    st.dataframe(
        filtered[
            [
                "procedure_id", "name", "category", "duration_minutes",
                "price_list", "base_cost", "commission_model", "commission_rate",
                "commission_fixed_value", "overhead_allocation_model", "price_min_recommended",
                "active", "version", "valid_from", "valid_to"
            ]
        ].sort_values(["name", "version"], ascending=[True, False]),
        use_container_width=True,
        hide_index=True,
    )

    st.caption("Selecione um procedimento para excluir (soft delete)")
    colx1, colx2 = st.columns([3, 1])
    with colx1:
        del_id = st.selectbox("procedure_id", ["-"] + filtered["procedure_id"].tolist())
    with colx2:
        if st.button("Excluir (soft)") and del_id and del_id != "-":
            soft_delete_procedure(del_id)
            st.success("Procedimento excluído (soft delete).")
            st.experimental_rerun()


def page_sales():
    st.header("Vendas / Execuções de Procedimentos")
    dfc = get_catalog()
    if dfc.empty:
        st.warning("Cadastre procedimentos antes de registrar vendas.")
        return

    active_items = dfc[(dfc["active"] == True) & (dfc["is_deleted"] == False)]
    options = active_items[["procedure_id", "name", "category", "price_list", "duration_minutes",
                            "commission_model", "commission_rate", "commission_fixed_value",
                            "commission_tiers", "bom_json", "base_cost",
                            "overhead_allocation_model", "overhead_rate_value"]]

    st.subheader("Lançar venda/execução")
    sel = st.selectbox("Procedimento", options.apply(lambda r: f"{r['name']} · {r['category']} (R$ {r['price_list']:.2f})", axis=1))
    idx = options.index[options.apply(lambda r: f"{r['name']} · {r['category']} (R$ {r['price_list']:.2f})", axis=1) == sel][0]
    row = options.loc[idx].to_dict()

    col1, col2, col3 = st.columns(3)
    with col1:
        sale_dt = st.datetime_input("Data/Hora da venda", value=datetime.now())
        discount_percent = st.number_input("Desconto (%)", min_value=0.0, max_value=100.0, step=1.0, value=0.0)
        gateway_fee_value = st.number_input("Gateway (R$)", min_value=0.0, step=1.0, value=0.0)
    with col2:
        professional = st.text_input("Profissional (opcional)")
        notes = st.text_input("Observações (opcional)")
    with col3:
        price_list_at_sale = float(row["price_list"]) if row.get("price_list") is not None else 0.0
        dsc = (discount_percent or 0.0) / 100.0
        sale_price = round(price_list_at_sale * (1 - dsc), 2)
        st.metric("Preço efetivo", f"R$ {sale_price:,.2f}")

    # preview dinâmico
    base_cost = float(row.get("base_cost") or 0)
    pv = calc_preview(
        sale_price=sale_price,
        base_cost=base_cost,
        commission_model=row.get("commission_model"),
        commission_rate=row.get("commission_rate"),
        commission_fixed_value=row.get("commission_fixed_value"),
        commission_tiers=row.get("commission_tiers"),
        overhead_model=row.get("overhead_allocation_model"),
        overhead_rate_value=row.get("overhead_rate_value"),
        duration_minutes=float(row.get("duration_minutes") or 0),
        gateway_fee_value=gateway_fee_value,
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Comissão (R$)", f"{pv['commission_value']:.2f}")
    m2.metric("Overhead (R$)", f"{pv['overhead_value']:.2f}")
    m3.metric("Lucro líquido (R$)", f"{pv['net_profit']:.2f}")
    m4.metric("Margem (%)", f"{pv['margin_percent']:.2f}%")

    if st.button("Registrar venda/execução", type="primary", use_container_width=True):
        sale_row = {
            "procedure_sale_id": generate_uuid(),
            "procedure_id": row.get("procedure_id"),
            "name": options.loc[idx, "name"],
            "category": options.loc[idx, "category"],
            "professional": professional.strip() if professional else "",
            "sale_datetime": sale_dt.isoformat(),
            "price_list_at_sale": price_list_at_sale,
            "discount_percent": float(discount_percent or 0.0),
            "sale_price": float(sale_price or 0.0),
            "commission_model_applied": pv["commission_model_applied"],
            "commission_rate_applied": pv["commission_rate_applied"],
            "commission_value": pv["commission_value"],
            "base_cost_at_sale": float(base_cost or 0.0),
            "overhead_value": pv["overhead_value"],
            "gateway_fee_value": float(gateway_fee_value or 0.0),
            "net_profit": pv["net_profit"],
            "margin_percent": pv["margin_percent"],
            "source_type": "single",
            "package_id": "",
            "notes": notes.strip() if notes else "",
        }
        add_sale(sale_row)
        st.success("Venda registrada com sucesso.")

    st.divider()
    st.subheader("Histórico de vendas")
    dfs = get_sales()
    if dfs.empty:
        st.info("Sem vendas registradas.")
        return

    # filtros simples
    c1, c2 = st.columns(2)
    with c1:
        f_cat = st.selectbox("Categoria", ["(todas)"] + sorted(dfs["category"].dropna().unique().tolist()))
    with c2:
        f_name = st.text_input("Procedimento (busca)")

    filt = dfs.copy()
    if f_cat != "(todas)":
        filt = filt[filt["category"] == f_cat]
    if f_name:
        filt = filt[filt["name"].str.contains(f_name, case=False, na=False)]

    st.dataframe(
        filt.sort_values("sale_datetime", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


# ============================
# ENTRYPOINT – PÁGINA
# ============================

def render():
    tabs = st.tabs(["Catálogo", "Vendas/Execuções"])
    with tabs[0]:
        page_catalog()
    with tabs[1]:
        page_sales()


if __name__ == "__main__":
    st.set_page_config(page_title="Procedimentos | Zaya", layout="wide")
    render()
