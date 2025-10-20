# src/pages/15_Precificacao_Procedimento.py
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Optional, Dict, Tuple

import pandas as pd
import streamlit as st

# =============================================================================
# Configuração básica
# =============================================================================
st.set_page_config(page_title="Precificação de Procedimentos", layout="wide")
st.title("💸 Precificação de Procedimentos")

# =============================================================================
# Arquivos e candidatos de colunas
# =============================================================================
CATALOG_FILE = Path("data/silver/fact_procedure/catalog.parquet")
BOM_FILE = Path(
    "data/silver/fact_procedure/bom.parquet"
)  # opcional, caso persista uma BOM

PRODUCTS_FILES = [
    Path("data/silver/fact_product_purchase/purchases.parquet"),
    Path("data/silver/fact_product/products.parquet"),
    Path("data/bronze/products.parquet"),
    Path("data/silver/products.parquet"),
    Path("data/products.parquet"),
]

NAME_CANDIDATES = ["nome", "name", "product_name", "title", "descricao", "descrição"]
COST_CANDIDATES = [
    "valor_compra",
    "purchase_price",
    "unit_cost",
    "cost",
    "price",
    "valor",
    "custo",
]
CATEGORY_CANDIDATES = ["categoria", "category"]


# =============================================================================
# Loaders utilitários (cacheados)
# =============================================================================
@st.cache_data(show_spinner=False)
def _load_df(path: Path) -> pd.DataFrame:
    """Lê Parquet se existir; retorna DF vazio se não existir ou falhar."""
    try:
        if path.exists():
            df = pd.read_parquet(path)
            if isinstance(df, pd.DataFrame):
                return df
    except Exception:
        pass
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def _load_products() -> Tuple[pd.DataFrame, str]:
    """Lê insumos de várias fontes (Parquet conhecido ou data layer). Retorna (df, fonte)."""
    for p in PRODUCTS_FILES:
        df = _load_df(p)
        if not df.empty:
            return df.copy(), f"parquet:{p}"
    # fallback: data layer
    try:
        from src.dataio.products import read_products as _read_products_dl  # type: ignore

        df = _read_products_dl(None)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df.copy(), "data_layer:src.dataio.products.read_products"
    except Exception:
        pass
    return pd.DataFrame(), "(nenhuma fonte encontrada)"


# =============================================================================
# Helpers genéricos
# =============================================================================
def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _get_value(row: pd.Series, candidates: List[str], default=None):
    for c in candidates:
        if c in row.index and pd.notna(row[c]):
            return row[c]
    return default


# =============================================================================
# Modelo de comissão e cálculos
# =============================================================================
CommissionModel = Literal["percent", "fixed", "tiered"]


@dataclass
class Tier:
    min: float
    max: float
    rate: float  # 0-1


def commission_value(
    price: float,
    model: CommissionModel,
    rate: float = 0.0,
    fixed: float = 0.0,
    tiers: Optional[List[Dict]] = None,
) -> float:
    """Comissão em R$, conforme modelo (percent/fixed/tiered)."""
    if price <= 0:
        return 0.0
    if model == "percent":
        return float(rate) * float(price)
    if model == "fixed":
        return float(fixed)
    if model == "tiered":
        if not tiers:
            return 0.0
        for t in tiers:
            tmin = float(t.get("min", 0))
            tmax = float(t.get("max", 1e18))
            trate = float(t.get("rate", 0.0))
            if tmin <= price <= tmax:
                return trate * price
        return float(tiers[-1].get("rate", 0.0)) * price
    return 0.0


def fees_value(
    price: float, gateway_rate: float, discount_rate: float, taxes_rate: float
) -> tuple[float, float]:
    """
    Gateway + Impostos como % do preço após desconto.
    Retorna (fees_total_em_R$, preco_apos_desconto).
    """
    price_after_disc = float(price) * (1.0 - float(discount_rate))
    gateway = float(gateway_rate) * price_after_disc
    taxes = float(taxes_rate) * price_after_disc
    return gateway + taxes, price_after_disc


def overhead_value(bom_cost: float, oh_fixed: float, oh_rate: float) -> float:
    """Overhead = fixo (R$) + percentual (% sobre o BOM)."""
    return float(oh_fixed) + (float(oh_rate) * float(bom_cost))


def net_profit(
    price: float,
    bom_cost: float,
    model: CommissionModel,
    rate: float,
    fixed_comm: float,
    tiers: Optional[List[Dict]],
    gateway_rate: float,
    discount_rate: float,
    taxes_rate: float,
    oh_fixed: float,
    oh_rate: float,
) -> float:
    """
    Lucro líquido em R$:
    Lucro = (Preço*(1-Desconto) - Gateway - Impostos) - (BOM + Overhead + Comissão).
    """
    comm = commission_value(price, model, rate, fixed_comm, tiers)
    fees, price_after_disc = fees_value(price, gateway_rate, discount_rate, taxes_rate)
    oh = overhead_value(bom_cost, oh_fixed, oh_rate)
    revenue_net = price_after_disc - fees
    profit = revenue_net - (bom_cost + oh + comm)
    return float(profit)


def margin(price: float, *args, **kwargs) -> float:
    """Margem líquida (profit / price)."""
    if price <= 0:
        return 0.0
    profit = net_profit(price, *args, **kwargs)
    return max(-1.0, min(1.0, profit / price))


def solve_price_for_target_margin(
    target_margin: float,
    bom_cost: float,
    model: CommissionModel,
    rate: float,
    fixed_comm: float,
    tiers: Optional[List[Dict]],
    gateway_rate: float,
    discount_rate: float,
    taxes_rate: float,
    oh_fixed: float,
    oh_rate: float,
    price_hint: float = 100.0,
) -> float:
    """Busca numérica (bisseção) para achar preço que atinge a margem-alvo (0-1)."""
    lo = max(bom_cost, 1.0)
    hi = max(price_hint, lo * 5)

    # aumenta hi até alcançar a margem-alvo
    for _ in range(40):
        m = margin(
            hi,
            bom_cost,
            model,
            rate,
            fixed_comm,
            tiers,
            gateway_rate,
            discount_rate,
            taxes_rate,
            oh_fixed,
            oh_rate,
        )
        if m >= target_margin - 1e-6:
            break
        hi *= 1.5

    # bisseção
    for _ in range(60):
        mid = 0.5 * (lo + hi)
        m = margin(
            mid,
            bom_cost,
            model,
            rate,
            fixed_comm,
            tiers,
            gateway_rate,
            discount_rate,
            taxes_rate,
            oh_fixed,
            oh_rate,
        )
        if m < target_margin:
            lo = mid
        else:
            hi = mid
        if abs(hi - lo) < 0.01:
            return round(mid, 2)
    return round(0.5 * (lo + hi), 2)


# =============================================================================
# Carregar bases (ANTES de usar)
# =============================================================================
catalog = _load_df(CATALOG_FILE)
bom_df = _load_df(BOM_FILE)
products_df, products_source = _load_products()

# =============================================================================
# UI — Layout
# =============================================================================
colL, colR = st.columns([1.15, 1])

# -----------------------------------------------------------------------------
# (L) Identificação, BOM (uma única vez), Overhead e Regras
# -----------------------------------------------------------------------------
with colL:
    st.subheader("1) Identificação")

    # Seleção opcional pelo catálogo
    if not catalog.empty:
        name_col_cat = _pick_col(catalog, ["name"])
        category_col_cat = _pick_col(catalog, ["category"])
        cat_opts = ["(digitar manualmente)"]
        if name_col_cat:
            cat_opts += sorted(
                catalog[name_col_cat].dropna().astype(str).unique().tolist()
            )
        sel_name = st.selectbox("Procedimento (catálogo opcional)", cat_opts, index=0)
        if name_col_cat and sel_name != "(digitar manualmente)":
            row = catalog[catalog[name_col_cat].astype(str) == sel_name].iloc[0]
        else:
            row = pd.Series({})
    else:
        row = pd.Series({})
        st.caption("Catálogo não encontrado (opcional).")

    name = st.text_input("Nome do procedimento", str(row.get("name", "")))
    category = st.text_input("Categoria", str(row.get("category", "")))

    # ------------------------ BOM (única seção) ------------------------
    st.subheader("2) BOM (insumos) a partir dos produtos cadastrados")

    bom_items: list[dict] = []
    bom_cost_from_products = 0.0

    with st.expander("🩺 Diagnóstico de insumos", expanded=False):
        st.caption(f"Fonte: **{products_source}**")
        if products_df.empty:
            st.warning(
                "Nenhum insumo carregado das fontes conhecidas. Você pode usar a BOM manual abaixo."
            )
        else:
            st.write("Colunas disponíveis:", list(products_df.columns)[:50])
            st.dataframe(
                products_df.head(10), use_container_width=True, hide_index=True
            )

    if products_df.empty:
        st.info(
            "Não encontrei insumos cadastrados. Use a tabela abaixo para compor a BOM manualmente."
        )
        bom_manual = st.data_editor(
            pd.DataFrame([{"item": "", "qtd": 1.0, "custo_unit": 0.0}]),
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
        )
        if not bom_manual.empty:
            bom_manual["subtotal"] = pd.to_numeric(
                bom_manual["qtd"], errors="coerce"
            ).fillna(0.0) * pd.to_numeric(
                bom_manual["custo_unit"], errors="coerce"
            ).fillna(0.0)
            bom_items = bom_manual.to_dict("records")
            bom_cost_from_products = float(bom_manual["subtotal"].sum())
    else:
        # autodetecta colunas e permite override manual
        auto_name_col = _pick_col(products_df, NAME_CANDIDATES) or ""
        auto_cost_col = _pick_col(products_df, COST_CANDIDATES) or ""

        csel1, csel2 = st.columns(2)
        with csel1:
            name_col_opt = st.selectbox(
                "Coluna de Nome",
                options=["(auto)"] + list(products_df.columns),
                index=0,
            )
        with csel2:
            cost_col_opt = st.selectbox(
                "Coluna de Custo",
                options=["(auto)"] + list(products_df.columns),
                index=0,
            )

        name_col = auto_name_col if name_col_opt == "(auto)" else name_col_opt
        cost_col = auto_cost_col if cost_col_opt == "(auto)" else cost_col_opt

        if (
            not name_col
            or not cost_col
            or name_col not in products_df.columns
            or cost_col not in products_df.columns
        ):
            st.warning(
                "Não consegui identificar Nome/Custo automaticamente. Use a BOM manual abaixo."
            )
            bom_manual = st.data_editor(
                pd.DataFrame([{"item": "", "qtd": 1.0, "custo_unit": 0.0}]),
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
            )
            if not bom_manual.empty:
                bom_manual["subtotal"] = pd.to_numeric(
                    bom_manual["qtd"], errors="coerce"
                ).fillna(0.0) * pd.to_numeric(
                    bom_manual["custo_unit"], errors="coerce"
                ).fillna(0.0)
                bom_items = bom_manual.to_dict("records")
                bom_cost_from_products = float(bom_manual["subtotal"].sum())
        else:
            # escolher insumos e quantidades
            options = sorted(
                products_df[name_col].dropna().astype(str).unique().tolist()
            )
            sel = st.multiselect("Selecione os insumos", options)

            if sel:
                st.write("Quantidades por item e custo unitário (média simples):")
                qty_inputs: dict[str, tuple[float, float]] = {}

                for item in sel:
                    match = products_df[products_df[name_col].astype(str) == str(item)]
                    unit_cost = float(
                        pd.to_numeric(match[cost_col], errors="coerce").mean() or 0.0
                    )

                    c1, c2, c3 = st.columns([0.6, 0.4, 1.0])
                    with c1:
                        st.caption(f"• {item}")
                    with c2:
                        q = st.number_input(
                            f"Qtd – {item}",
                            min_value=0.0,
                            step=0.1,
                            value=1.0,
                            key=f"q_{item}",
                        )
                    with c3:
                        st.caption(f"Custo unit.: R$ {unit_cost:,.2f}")

                    qty_inputs[item] = (q, unit_cost)

                for item, (q, unit_cost) in qty_inputs.items():
                    subtotal = float(q) * float(unit_cost)
                    bom_items.append(
                        {
                            "item": item,
                            "qtd": q,
                            "custo_unit": unit_cost,
                            "subtotal": subtotal,
                        }
                    )
                bom_cost_from_products = sum(x["subtotal"] for x in bom_items)

                with st.expander("Detalhe da BOM (itens × custo)", expanded=False):
                    st.dataframe(
                        pd.DataFrame(bom_items),
                        use_container_width=True,
                        hide_index=True,
                    )

    # custo de BOM manual com default = total estimado
    st.subheader("3) Overhead")
    bom_cost = st.number_input(
        "Custo de insumos (BOM) – R$",
        min_value=0.0,
        step=0.01,
        value=float(bom_cost_from_products or 0.0),
        help="Soma dos insumos selecionados acima. Você pode ajustar manualmente.",
    )
    oh_fixed = st.number_input(
        "Overhead fixo (R$)", min_value=0.0, step=0.01, value=0.0
    )
    oh_rate = st.number_input(
        "Overhead (%) sobre BOM", min_value=0.0, max_value=1.0, step=0.01, value=0.0
    )

    st.subheader("4) Comissão / Fees / Impostos")
    model = st.radio(
        "Modelo de comissão", ["percent", "fixed", "tiered"], horizontal=True, index=0
    )
    c1, c2 = st.columns(2)
    with c1:
        rate = st.number_input(
            "% Comissão (0-1)", min_value=0.0, max_value=1.0, step=0.01, value=0.30
        )
        fixed_comm = st.number_input(
            "Comissão fixa (R$)", min_value=0.0, step=0.01, value=0.0
        )
    with c2:
        gateway_rate = st.number_input(
            "Gateway (%)", min_value=0.0, max_value=1.0, step=0.005, value=0.03
        )
        discount_rate = st.number_input(
            "Desconto (%)", min_value=0.0, max_value=1.0, step=0.01, value=0.0
        )

    taxes_rate = st.number_input(
        "Impostos (%)", min_value=0.0, max_value=1.0, step=0.01, value=0.0
    )

    tiers_text = st.text_area(
        "Tiers (JSON) – se usar modelo 'tiered'",
        value='[{"min":0,"max":500,"rate":0.20},{"min":500,"max":999999,"rate":0.30}]',
        height=100,
    )
    try:
        tiers = json.loads(tiers_text) if model == "tiered" else None
        if tiers is not None and not isinstance(tiers, list):
            tiers = None
            st.warning("JSON de tiers inválido — usando None.")
    except Exception:
        tiers = None
        st.warning("JSON de tiers inválido — usando None.")

# -----------------------------------------------------------------------------
# (R) Estratégia de precificação
# -----------------------------------------------------------------------------
with colR:
    st.subheader("5) Estratégia")
    strategy = st.radio(
        "Escolha o modo",
        ["Descobrir preço por margem-alvo", "Avaliar um preço existente"],
        index=0,
    )

    if strategy == "Descobrir preço por margem-alvo":
        target_margin = st.slider("Margem-alvo (%)", 0.0, 90.0, 30.0, 1.0) / 100.0
        hint = st.number_input(
            "Chute inicial (R$)",
            min_value=0.0,
            step=1.0,
            value=max(100.0, float(bom_cost) * 2.0),
            help="Só um ponto de partida para a busca numérica.",
        )

        if st.button(
            "Calcular preço recomendado", type="primary", use_container_width=True
        ):
            price = solve_price_for_target_margin(
                target_margin=target_margin,
                bom_cost=float(bom_cost),
                model=model,
                rate=float(rate),
                fixed_comm=float(fixed_comm),
                tiers=tiers,
                gateway_rate=float(gateway_rate),
                discount_rate=float(discount_rate),
                taxes_rate=float(taxes_rate),
                oh_fixed=float(oh_fixed),
                oh_rate=float(oh_rate),
                price_hint=float(hint),
            )

            # métricas e breakdown
            comm = commission_value(price, model, rate, fixed_comm, tiers)
            fees, price_after_disc = fees_value(
                price, gateway_rate, discount_rate, taxes_rate
            )
            oh = overhead_value(bom_cost, oh_fixed, oh_rate)
            _profit = net_profit(
                price,
                float(bom_cost),
                model,
                float(rate),
                float(fixed_comm),
                tiers,
                float(gateway_rate),
                float(discount_rate),
                float(taxes_rate),
                float(oh_fixed),
                float(oh_rate),
            )
            _margin = (
                margin(
                    price,
                    float(bom_cost),
                    model,
                    float(rate),
                    float(fixed_comm),
                    tiers,
                    float(gateway_rate),
                    float(discount_rate),
                    float(taxes_rate),
                    float(oh_fixed),
                    float(oh_rate),
                )
                * 100.0
            )

            st.toast(f"Preço recomendado: R$ {price:,.2f}", icon="✨")
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Preço de tabela", f"R$ {price:,.2f}")
            cB.metric("Preço após desconto", f"R$ {price_after_disc:,.2f}")
            cC.metric("Margem (%)", f"{_margin:.2f}%")
            cD.metric("Lucro líquido", f"R$ {_profit:,.2f}")

            with st.expander("📊 Decomposição de custos e resultado", expanded=True):
                st.dataframe(
                    pd.DataFrame(
                        [
                            {"Item": "Comissão", "Valor (R$)": comm},
                            {"Item": "Gateway + Impostos", "Valor (R$)": fees},
                            {"Item": "BOM (insumos)", "Valor (R$)": float(bom_cost)},
                            {"Item": "Overhead", "Valor (R$)": oh},
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

    else:
        price_informed = st.number_input(
            "Preço informado (R$)",
            min_value=0.0,
            step=0.5,
            value=max(0.0, float(bom_cost) * 2.0),
        )

        if st.button("Calcular margem/lucro", type="primary", use_container_width=True):
            comm = commission_value(price_informed, model, rate, fixed_comm, tiers)
            fees, price_after_disc = fees_value(
                price_informed, gateway_rate, discount_rate, taxes_rate
            )
            oh = overhead_value(bom_cost, oh_fixed, oh_rate)
            _profit = net_profit(
                price_informed,
                float(bom_cost),
                model,
                float(rate),
                float(fixed_comm),
                tiers,
                float(gateway_rate),
                float(discount_rate),
                float(taxes_rate),
                float(oh_fixed),
                float(oh_rate),
            )
            _margin = (
                margin(
                    price_informed,
                    float(bom_cost),
                    model,
                    float(rate),
                    float(fixed_comm),
                    tiers,
                    float(gateway_rate),
                    float(discount_rate),
                    float(taxes_rate),
                    float(oh_fixed),
                    float(oh_rate),
                )
                * 100.0
            )

            st.toast(f"Margem: {_margin:.2f}%  •  Lucro: R$ {_profit:,.2f}", icon="✅")
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Preço de tabela", f"R$ {price_informed:,.2f}")
            cB.metric("Preço após desconto", f"R$ {price_after_disc:,.2f}")
            cC.metric("Margem (%)", f"{_margin:.2f}%")
            cD.metric("Lucro líquido", f"R$ {_profit:,.2f}")

            with st.expander("📊 Decomposição de custos", expanded=True):
                st.dataframe(
                    pd.DataFrame(
                        [
                            {"Item": "Comissão", "Valor (R$)": comm},
                            {"Item": "Gateway + Impostos", "Valor (R$)": fees},
                            {"Item": "BOM (insumos)", "Valor (R$)": float(bom_cost)},
                            {"Item": "Overhead", "Valor (R$)": oh},
                        ]
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

# =============================================================================
# Explicação das variáveis
# =============================================================================
st.divider()
with st.expander(
    "📖 Explicação das variáveis (o que preencher e como afeta o cálculo)",
    expanded=False,
):
    st.markdown(
        """
**Identificação**
- **Nome do procedimento** e **Categoria**: servem para referência/relatórios (podem vir do catálogo).

**BOM (insumos)**
- **Insumos selecionados**: itens já cadastrados no módulo de produtos.
  Para cada item, informe a **Quantidade**. O sistema usa o **custo unitário médio** (dos seus registros) e soma `qtd × custo_unit` → **BOM (R$)**.
- **Custo de insumos (BOM) – R$**: campo numérico com a soma (você pode ajustar manualmente).

**Overhead**
- **Overhead fixo (R$)**: custos indiretos por execução (ex.: esterilização, setup, EPI genérico, limpeza).
- **Overhead (%) sobre BOM**: adicional proporcional ao custo de insumos (ex.: 0.15 = 15%).
  **Overhead total = fixo + (% × BOM).**

**Comissão / Fees / Impostos**
- **Modelo de comissão**:
  - `percent`: comissão = `rate × preço`.
  - `fixed`: comissão = valor fixo em R$.
  - `tiered`: comissão percentual conforme faixa de preço (JSON em `tiers`).
- **% Comissão (0–1)**: ex.: 0.30 = 30% do preço.
- **Comissão fixa (R$)**: valor absoluto (se usar `fixed`).
- **Gateway (%)**: taxa do meio de pagamento (ex.: 0.03 = 3%).
- **Desconto (%)**: desconto concedido ao cliente (reduz a receita).
- **Impostos (%)**: ISS/retidos conforme sua regra (simplificado).

**Estratégia**
- **Descobrir preço por margem-alvo**: informe **Margem-alvo** (0–1) e um **chute**; o sistema calcula o **preço recomendado**.
- **Avaliar um preço existente**: informe o **preço** e o sistema calcula **margem** e **lucro**.

**Fórmulas principais**
- `Preço após desconto = Preço × (1 − Desconto)`
- `Fees (R$) = Gateway × (Preço após desconto) + Impostos × (Preço após desconto)`
- `Overhead (R$) = Overhead fixo + (Overhead % × BOM)`
- `Comissão (R$)` = conforme modelo
- `Lucro líquido = (Preço após desconto − Fees) − (BOM + Overhead + Comissão)`
- `Margem = Lucro líquido / Preço`
        """
    )
