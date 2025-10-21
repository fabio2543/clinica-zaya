# src/pages/16_Simulador_Comissionamento.py
from __future__ import annotations

from pathlib import Path
import json
from typing import List, Optional, Dict, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Simulador | Comissão & Break-even", layout="wide")
st.title("📈 Simulador de Comissionamento & Break-even")

# =============================================================================
# Arquivos padrão (ajuste caminhos conforme seu projeto)
# =============================================================================
FIXED_COSTS_FILES = [
    Path("data/silver/fixed_costs.parquet"),
    Path("data/silver/fact_fixed_costs/fixed_costs.parquet"),
    Path("data/silver/dim_fixed_costs.parquet"),
]

# candidatos de nomes de colunas na base de precificação
COL_PRICE = ["preco", "preço", "price", "valor", "tabela"]
COL_DISCOUNT = ["desconto", "discount", "disc"]
COL_GATEWAY = ["gateway", "taxa_gateway", "adquirente"]
COL_TAXES = ["impostos", "iss", "taxes"]
COL_BOM = ["custo_bom", "bom", "custo_insumos", "insumos", "custo_direto"]
COL_OH_FIXED = ["overhead_fixo", "oh_fixo", "custo_indireto_fixo"]
COL_OH_RATE = ["overhead_pct", "oh_pct", "overhead_percent", "overhead_%"]
COL_COMM_RATE = [
    "comissao_pct",
    "comissão_%",
    "commission_pct",
    "comissao",
    "commission",
]
COL_COMM_FIXED = ["comissao_fixa", "commission_fixed"]
COL_COMM_MODEL = ["modelo_comissao", "commission_model", "modelo"]
COL_NAME = ["procedimento", "nome", "name", "procedure"]
COL_CATEGORY = ["categoria", "category"]


# =============================================================================
# Utils
# =============================================================================
def _pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


@st.cache_data(show_spinner=False)
def _load_parquet(path: Path) -> pd.DataFrame:
    try:
        if path.exists():
            return pd.read_parquet(path)
    except Exception:
        pass
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def _load_fixed_costs() -> Tuple[float, str]:
    """Tenta detectar custos fixos totais a partir de Parquets conhecidos."""
    for p in FIXED_COSTS_FILES:
        df = _load_parquet(p)
        if not df.empty:
            # tenta somar colunas típicas
            for col in ["valor", "value", "amount", "custo", "cost"]:
                if col in df.columns:
                    total = pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum()
                    return float(total), f"parquet:{p}"
            # fallback: soma tudo que for numérico
            total = df.select_dtypes("number").sum(numeric_only=True).sum()
            return float(total), f"parquet:{p}"
    return 0.0, "(não encontrado)"


def _rate_from_revenue(revenue: float, tiers: List[Dict]) -> float:
    """
    Faixas no formato: [{'min': 0, 'rate': 0.10}, {'min': 25000, 'rate': 0.15}, ...]
    Retorna a MAIOR taxa cuja 'min' seja <= faturamento.
    """
    if not tiers:
        return 0.0
    tiers_sorted = sorted(
        [
            {"min": float(t.get("min", 0)), "rate": float(t.get("rate", 0))}
            for t in tiers
        ],
        key=lambda x: x["min"],
    )
    rate = tiers_sorted[0]["rate"]
    for t in tiers_sorted:
        if revenue >= t["min"]:
            rate = t["rate"]
        else:
            break
    return float(rate)


# =============================================================================
# Sidebar – Cenário e Comissão por Faixa
# =============================================================================
with st.sidebar:
    st.header("⚙️ Cenário (opcional)")
    st.caption(
        "Preencha para **sobrescrever** os valores da base ao simular. Deixe 0 para manter os valores originais."
    )
    scenario_discount = st.number_input(
        "Desconto (%)",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.0,
        format="%.2f",
    )
    scenario_gateway = st.number_input(
        "Gateway (%)",
        min_value=0.0,
        max_value=1.0,
        step=0.005,
        value=0.0,
        format="%.3f",
    )
    scenario_taxes = st.number_input(
        "Impostos (%)",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.0,
        format="%.2f",
    )
    scenario_oh_fixed = st.number_input(
        "Overhead fixo (R$)", min_value=0.0, step=0.50, value=0.0, format="%.2f"
    )
    scenario_oh_rate = st.number_input(
        "Overhead (%) sobre BOM",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.0,
        format="%.2f",
    )

    st.divider()
    st.caption("Comissão")
    scenario_comm_model = st.selectbox(
        "Modelo", ["(usar da base)", "percent", "fixed", "tiered"], index=0
    )
    scenario_comm_rate = st.number_input(
        "% Comissão (0-1)",
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        value=0.0,
        format="%.2f",
    )
    scenario_comm_fixed = st.number_input(
        "Comissão fixa (R$)", min_value=0.0, step=0.50, value=0.0, format="%.2f"
    )
    tiers_text = st.text_area(
        "Tiers (JSON) – se usar 'tiered'",
        value='[{"min":0,"max":500,"rate":0.20},{"min":500,"max":999999,"rate":0.30}]',
        height=100,
    )

    st.divider()
    st.header("🪜 Comissão por Faixa (Faturamento Mensal)")
    use_revenue_tiers = st.toggle(
        "Ativar comissão global por faturamento",
        value=False,
        help="Se ativo, a comissão passa a ser a taxa da faixa correspondente ao faturamento mensal previsto.",
    )

    default_tiers_df = pd.DataFrame(
        [
            {"min": 0.0, "rate": 0.10},
            {"min": 25000.0, "rate": 0.125},
            {"min": 60000.0, "rate": 0.175},
            {"min": 100000.0, "rate": 0.225},  # mantém 20% acima de 100k
        ]
    )
    tiers_df = st.data_editor(
        default_tiers_df,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "min": st.column_config.NumberColumn(
                "Faturamento mínimo (R$)", min_value=0.0, step=1000.0
            ),
            # sem "%" no format; exibimos decimal
            "rate": st.column_config.NumberColumn(
                "Taxa (decimal)", min_value=0.0, max_value=1.0, step=0.01
            ),
        },
        disabled=not use_revenue_tiers,
        key="tiers_revenue_df",
    )

    faturamento_previsto = st.number_input(
        "Faturamento mensal previsto (R$)",
        min_value=0.0,
        step=1000.0,
        value=0.0,
        format="%.2f",
        help="Se deixar 0, o sistema usa automaticamente o faturamento simulado.",
        disabled=not use_revenue_tiers,
    )

    # Apenas capturamos as faixas; a taxa será definida depois (quando soubermos o faturamento)
    if use_revenue_tiers:
        tiers_list_global = tiers_df.to_dict("records")
    else:
        tiers_list_global = None


# =============================================================================
# Upload da Base de Precificação
# =============================================================================
st.subheader("1) Carregar Precificação Base (CSV/XLSX)")

uploaded = st.file_uploader(
    "Selecione sua base (uma linha por procedimento)", type=["csv", "xlsx"]
)
if uploaded is None:
    st.info(
        "Dica: colunas típicas esperadas → preço, desconto, gateway, impostos, custo_bom, "
        "overhead_fixo, overhead_pct, comissao_pct/comissao_fixa/modelo, procedimento, categoria."
    )
    st.stop()

try:
    if uploaded.name.lower().endswith(".csv"):
        base = pd.read_csv(uploaded)
    else:
        base = pd.read_excel(uploaded)
except Exception as e:
    st.error("Não foi possível ler o arquivo.")
    st.exception(e)
    st.stop()

if base.empty:
    st.warning("Arquivo sem dados.")
    st.stop()

st.success(f"Base carregada: {len(base)} linhas.")
with st.expander("👀 Preview da Base", expanded=False):
    st.dataframe(base.head(20), use_container_width=True)


# =============================================================================
# Mapear colunas (autodetect + escolha manual)
# =============================================================================
st.subheader("2) Mapear colunas")
auto = {
    "name": _pick_col(base, COL_NAME),
    "category": _pick_col(base, COL_CATEGORY),
    "price": _pick_col(base, COL_PRICE),
    "discount": _pick_col(base, COL_DISCOUNT),
    "gateway": _pick_col(base, COL_GATEWAY),
    "taxes": _pick_col(base, COL_TAXES),
    "bom": _pick_col(base, COL_BOM),
    "oh_fixed": _pick_col(base, COL_OH_FIXED),
    "oh_rate": _pick_col(base, COL_OH_RATE),
    "comm_model": _pick_col(base, COL_COMM_MODEL),
    "comm_rate": _pick_col(base, COL_COMM_RATE),
    "comm_fixed": _pick_col(base, COL_COMM_FIXED),
}

cols = st.columns(4)
keys = list(auto.keys())
for i, k in enumerate(keys):
    with cols[i % 4]:
        opts = ["(nenhuma)"] + list(base.columns)
        idx = opts.index(auto[k]) if auto[k] in opts else 0
        auto[k] = st.selectbox(f"Coluna: {k}", opts, index=idx)

req = ["price", "bom"]
missing = [k for k in req if auto[k] == "(nenhuma)"]
if missing:
    st.error(f"Colunas obrigatórias não mapeadas: {missing}.")
    st.stop()


# =============================================================================
# Funções de cálculo (iguais às do módulo de precificação unitária)
# =============================================================================
def commission_value(
    price: float,
    model: str,
    rate: float = 0.0,
    fixed: float = 0.0,
    tiers: Optional[List[Dict]] = None,
) -> float:
    if price <= 0:
        return 0.0
    model = (model or "").strip().lower()
    if model == "percent":
        return float(rate) * float(price)
    if model == "fixed":
        return float(fixed)
    if model == "tiered":
        tiers = tiers or []
        for t in tiers:
            tmin = float(t.get("min", 0))
            tmax = float(t.get("max", 1e18))
            trate = float(t.get("rate", 0.0))
            if tmin <= price <= tmax:
                return trate * price
        return (float(tiers[-1].get("rate", 0.0)) * price) if tiers else 0.0
    return 0.0


def fees_value(
    price: float, gateway_rate: float, discount_rate: float, taxes_rate: float
) -> tuple[float, float]:
    price_after_disc = float(price) * (1.0 - float(discount_rate))
    gateway = float(gateway_rate) * price_after_disc
    taxes = float(taxes_rate) * price_after_disc
    return gateway + taxes, price_after_disc


def overhead_value(bom_cost: float, oh_fixed: float, oh_rate: float) -> float:
    return float(oh_fixed) + (float(oh_rate) * float(bom_cost))


def net_profit(
    price: float,
    bom_cost: float,
    model: str,
    rate: float,
    fixed_comm: float,
    tiers: Optional[List[Dict]],
    gateway_rate: float,
    discount_rate: float,
    taxes_rate: float,
    oh_fixed: float,
    oh_rate: float,
) -> float:
    comm = commission_value(price, model, rate, fixed_comm, tiers)
    fees, price_after_disc = fees_value(price, gateway_rate, discount_rate, taxes_rate)
    oh = overhead_value(bom_cost, oh_fixed, oh_rate)
    revenue_net = price_after_disc - fees
    profit = revenue_net - (bom_cost + oh + comm)
    return float(profit)


def margin(price: float, *args, **kwargs) -> float:
    if price <= 0:
        return 0.0
    return max(-1.0, min(1.0, net_profit(price, *args, **kwargs) / price))


# =============================================================================
# Aplicar cenário e calcular resultados (unitários)
# =============================================================================
def _val(row, key, default=0.0):
    col = auto.get(key)
    if not col or col == "(nenhuma)":
        return default
    v = row.get(col)
    return float(pd.to_numeric(v, errors="coerce")) if pd.notna(v) else default


def _text(row, key, default=""):
    col = auto.get(key)
    if not col or col == "(nenhuma)":
        return default
    v = row.get(col)
    return str(v) if pd.notna(v) else default


# tiers (se cenário tiered estiver ativo)
try:
    tiers_scn = json.loads(tiers_text) if scenario_comm_model == "tiered" else None
    if tiers_scn is not None and not isinstance(tiers_scn, list):
        tiers_scn = None
except Exception:
    tiers_scn = None

rows: List[Dict] = []
for _, r in base.iterrows():
    name = _text(r, "name")
    category = _text(r, "category")

    price = _val(r, "price", 0.0)
    discount = scenario_discount if scenario_discount > 0 else _val(r, "discount", 0.0)
    gateway = scenario_gateway if scenario_gateway > 0 else _val(r, "gateway", 0.0)
    taxes = scenario_taxes if scenario_taxes > 0 else _val(r, "taxes", 0.0)

    bom = _val(r, "bom", 0.0)
    oh_fixed = scenario_oh_fixed if scenario_oh_fixed > 0 else _val(r, "oh_fixed", 0.0)
    oh_rate = scenario_oh_rate if scenario_oh_rate > 0 else _val(r, "oh_rate", 0.0)

    # comissão: base -> cenário -> (se faixa ativa, taxa será aplicada depois)
    comm_model = (
        scenario_comm_model
        if scenario_comm_model != "(usar da base)"
        else _text(r, "comm_model", "")
    )
    comm_rate = (
        scenario_comm_rate if scenario_comm_rate > 0 else _val(r, "comm_rate", 0.0)
    )
    comm_fixed = (
        scenario_comm_fixed if scenario_comm_fixed > 0 else _val(r, "comm_fixed", 0.0)
    )

    if use_revenue_tiers and (tiers_list_global is not None):
        # taxa ainda não é conhecida aqui; aplicaremos depois
        comm_model = "percent"
        comm_rate = 0.0
        comm_fixed = 0.0
        tiers_eff = None
    else:
        tiers_eff = tiers_scn if comm_model == "tiered" else None

    # cálculo unitário (provisório, se faixa estiver ativa)
    fees, price_after_disc = fees_value(price, gateway, discount, taxes)
    oh = overhead_value(bom, oh_fixed, oh_rate)
    comm = commission_value(price, comm_model, comm_rate, comm_fixed, tiers_eff)
    profit = price_after_disc - fees - (bom + oh + comm)
    mg = profit / price if price > 0 else 0.0

    # contribuição unitária (para BE): preço líquido - fees - (BOM + OH%*BOM + comissão)
    contribution = price_after_disc - fees - (bom + (oh_rate * bom) + comm)

    rows.append(
        {
            "procedimento": name,
            "categoria": category,
            "preco": price,
            "preco_liq": price_after_disc,
            "desconto_%": discount,
            "gateway_%": gateway,
            "impostos_%": taxes,
            "bom": bom,
            "overhead_fixo": oh_fixed,
            "overhead_%": oh_rate,
            "comissao_modelo": comm_model or "(none)",
            "comissao_%": comm_rate,
            "comissao_fixa": comm_fixed,
            "comissao_R$": comm,
            "fees_R$": fees,
            "overhead_R$": oh,
            "lucro_R$": profit,
            "margem_%": mg,
            "contribuicao_R$": contribution,
        }
    )

result = pd.DataFrame(rows)

if use_revenue_tiers:
    st.info(
        "Comissão global por faixa ativada. A taxa efetiva será calculada a partir do faturamento mensal previsto (manual ou automático)."
    )

st.subheader("3) Resultados por procedimento")
st.dataframe(
    result[
        [
            "procedimento",
            "categoria",
            "preco",
            "preco_liq",
            "bom",
            "overhead_R$",
            "comissao_R$",
            "fees_R$",
            "lucro_R$",
            "margem_%",
            "contribuicao_R$",
        ]
    ].sort_values("margem_%", ascending=False),
    use_container_width=True,
    hide_index=True,
)

# KPIs iniciais (unitários)
colA, colB, colC, colD = st.columns(4)
colA.metric("Margem média (%)", f"{(result['margem_%'].mean() * 100):.2f}%")
colB.metric("Lucro médio por proc. (R$)", f"{result['lucro_R$'].mean():,.2f}")
colC.metric("Contribuição média (R$)", f"{result['contribuicao_R$'].mean():,.2f}")
colD.metric("Itens na base", f"{len(result)}")

# =============================================================================
# 4) Simulação de vendas (quantidades) + Break-even dinâmico
# =============================================================================
st.subheader("4) Simulação de vendas (quantidades) & Break-even")

# lembrar último modo para limpar estado ao trocar
if "sales_mode_last" not in st.session_state:
    st.session_state["sales_mode_last"] = None

mode = st.radio(
    "Modo de simulação de vendas",
    ["Por total + mix", "Tabela de quantidades"],
    horizontal=True,
    key="sales_mode_radio",
)

# limpa estado do editor ao alternar modo
if st.session_state["sales_mode_last"] != mode:
    for k in ["qty_tbl_manual"]:
        st.session_state.pop(k, None)
    st.session_state["sales_mode_last"] = mode

# Garante coluna 'mix' e normaliza (soma = 1)
if "mix" not in result.columns:
    n = max(1, len(result))
    result["mix"] = 1.0 / n

result["mix"] = (
    pd.to_numeric(result["mix"], errors="coerce").fillna(0.0).clip(lower=0.0)
)
soma_mix = float(result["mix"].sum())
if soma_mix <= 0:
    n = max(1, len(result))
    result["mix"] = 1.0 / n
else:
    result["mix"] = result["mix"] / soma_mix

# 4.1 – Custos fixos (detecção + input)
fc_detected, source = _load_fixed_costs()
c1, c2 = st.columns(2)
with c1:
    fixed_costs = st.number_input(
        "Custos Fixos totais (R$/mês)",
        min_value=0.0,
        step=100.0,
        value=fc_detected,
        help=f"Detectado automaticamente de {source}. Você pode ajustar.",
        format="%.2f",
    )

st.caption(
    "Defina as quantidades vendidas do mês para simular faturamento, margem e atingimento do BE."
)

# 4.2 – Quantidades (por total + mix OU manual)
if mode == "Por total + mix":
    total_units = st.number_input(
        "Unidades totais vendidas no mês",
        min_value=0.0,
        step=1.0,
        value=0.0,
        format="%.0f",
    )
    result["qtd_vendida"] = (float(total_units) * result["mix"]).fillna(0.0)
else:
    qty_editor = st.data_editor(
        result[["procedimento"]].assign(qtd_vendida=0.0),
        num_rows="fixed",
        use_container_width=True,
        hide_index=True,
        column_config={
            "qtd_vendida": st.column_config.NumberColumn(
                "Qtd vendida", min_value=0.0, step=1.0, format="%.0f"
            )
        },
        key="qty_tbl_manual",
    )
    result = result.drop(columns=["qtd_vendida"], errors="ignore").merge(
        qty_editor, on="procedimento", how="left"
    )
    result["qtd_vendida"] = pd.to_numeric(
        result["qtd_vendida"], errors="coerce"
    ).fillna(0.0)

# 4.3 – Totais realizados a partir das quantidades
result["fat_bruto_total"] = result["preco"] * result["qtd_vendida"]
result["fat_liq_total"] = result["preco_liq"] * result["qtd_vendida"]
result["fees_total"] = result["fees_R$"] * result["qtd_vendida"]
result["oh_total"] = result["overhead_R$"] * result["qtd_vendida"]
result["comissao_total"] = result["comissao_R$"] * result["qtd_vendida"]
result["lucro_total"] = result["lucro_R$"] * result["qtd_vendida"]
result["contrib_total"] = result["contribuicao_R$"] * result["qtd_vendida"]

fat_bruto_sum = float(result["fat_bruto_total"].sum())
lucro_sum = float(result["lucro_total"].sum())
contrib_sum = float(result["contrib_total"].sum())
unid_vendidas_sum = float(result["qtd_vendida"].sum())

# ===== Comissão por faixa (FINAL) =====
final_rate = None
if use_revenue_tiers and (tiers_list_global is not None):
    # se o usuário não informou faturamento previsto, usa o simulado
    if (faturamento_previsto is None) or (faturamento_previsto <= 0):
        faturamento_previsto = float(fat_bruto_sum)

    # taxa efetiva final baseada no faturamento previsto (manual ou automático)
    final_rate = _rate_from_revenue(float(faturamento_previsto), tiers_list_global)

    # Recalcular comissão e campos derivados com a taxa final (unitários e totais)
    result["comissao_R$"] = result["preco"] * float(final_rate)
    result["lucro_R$"] = (
        result["preco_liq"]
        - result["fees_R$"]
        - (result["bom"] + result["overhead_R$"] + result["comissao_R$"])
    )
    result["margem_%"] = result["lucro_R$"] / result["preco"].replace(0, pd.NA).fillna(
        0.0
    )
    result["contribuicao_R$"] = (
        result["preco_liq"]
        - result["fees_R$"]
        - (
            result["bom"]
            + (result["overhead_%"] * result["bom"])
            + result["comissao_R$"]
        )
    )

    result["comissao_total"] = result["comissao_R$"] * result["qtd_vendida"]
    result["lucro_total"] = result["lucro_R$"] * result["qtd_vendida"]
    result["contrib_total"] = result["contribuicao_R$"] * result["qtd_vendida"]

    # recompute agregados após o recálculo
    lucro_sum = float(result["lucro_total"].sum())
    contrib_sum = float(result["contrib_total"].sum())

# KPI de faturamento: previsto (manual/auto) quando faixa ativa; senão, realizado
if use_revenue_tiers:
    fat_bruto_kpi = float(
        faturamento_previsto
        if (faturamento_previsto and faturamento_previsto > 0)
        else fat_bruto_sum
    )
else:
    fat_bruto_kpi = float(fat_bruto_sum)

margem_realizada = (lucro_sum / fat_bruto_sum) if fat_bruto_sum > 0 else 0.0

# contribuição média planejada (mix planejado) e efetiva (nas vendas)
contrib_avg_planejado = (result["contribuicao_R$"] * result["mix"]).sum()
contrib_avg_efetivo = (
    (contrib_sum / unid_vendidas_sum)
    if unid_vendidas_sum > 0
    else contrib_avg_planejado
)

# BE teórico (com mix planejado)
beq_unidades_planejado = (
    (fixed_costs / contrib_avg_planejado) if contrib_avg_planejado > 0 else float("inf")
)

faltante_RS = max(fixed_costs - contrib_sum, 0.0)
faltam_unidades_efetivo = (
    (faltante_RS / contrib_avg_efetivo) if contrib_avg_efetivo > 0 else float("inf")
)
faltam_unidades_planejado = (
    (faltante_RS / contrib_avg_planejado) if contrib_avg_planejado > 0 else float("inf")
)

# KPIs principais
k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Faturamento bruto realizado (R$)",
    f"{fat_bruto_kpi:,.2f}",
    help="Com comissão global por faturamento, mostra o faturamento mensal previsto (manual ou automático).",
)
k2.metric("Lucro realizado (R$)", f"{lucro_sum:,.2f}")
k3.metric("Margem realizada (%)", f"{margem_realizada * 100:.2f}%")
k4.metric("Unidades vendidas", f"{unid_vendidas_sum:,.0f}")

k5, k6, k7 = st.columns(3)
k5.metric("Contribuição gerada (R$)", f"{contrib_sum:,.2f}")
k6.metric("Faltante p/ BE (R$)", f"{faltante_RS:,.2f}")
atingimento = (contrib_sum / fixed_costs) if fixed_costs > 0 else 0.0
k7.metric("Atingimento do BE (%)", f"{min(atingimento, 1.0) * 100:.2f}%")

k8, k9 = st.columns(2)
k8.metric(
    "Unid. adicionais p/ BE (mix efetivo)",
    f"{(0 if faltante_RS == 0 else faltam_unidades_efetivo):,.2f}",
)
k9.metric(
    "Unid. adicionais p/ BE (mix planejado)",
    f"{(0 if faltante_RS == 0 else faltam_unidades_planejado):,.2f}",
)

# (opcional) exibir a taxa efetiva no corpo principal
if use_revenue_tiers:
    st.metric("Taxa de comissão efetiva", f"{(final_rate or 0) * 100:.2f}%")

with st.expander("📊 Detalhe por procedimento (realizado no mês)", expanded=False):
    st.dataframe(
        result[
            [
                "procedimento",
                "qtd_vendida",
                "fat_bruto_total",
                "comissao_total",
                "fees_total",
                "oh_total",
                "lucro_total",
                "contrib_total",
            ]
        ].sort_values("fat_bruto_total", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

# =============================================================================
# Exportar
# =============================================================================
st.subheader("5) Exportar resultados")
csv = result.to_csv(index=False).encode("utf-8")
st.download_button(
    "⬇️ Baixar CSV com resultados",
    data=csv,
    file_name="simulacao_comissao_breakeven.csv",
    mime="text/csv",
)
