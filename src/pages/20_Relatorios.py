import pandas as pd
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Relatórios | Zaya", layout="wide")

DATA_DIR = Path("data/silver")
SALE_FILE = DATA_DIR / "fact_procedure_sale" / "sales.parquet"
CATALOG_FILE = DATA_DIR / "fact_procedure" / "catalog.parquet"

st.title("📊 Relatórios de Procedimentos")
st.caption(
    "Visão geral de receitas, comissões, lucro e margens. Os dados são lidos dos Parquets da camada silver."
)


@st.cache_data(show_spinner=False)
def load_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


sales = load_df(SALE_FILE)
catalog = load_df(CATALOG_FILE)

if sales.empty:
    st.warning(
        "Nenhuma venda encontrada em data/silver/fact_procedure_sale/sales.parquet"
    )
    st.stop()

# Filtros
c1, c2, c3 = st.columns(3)
with c1:
    cats = ["(todas)"] + sorted(
        [c for c in sales["category"].dropna().unique().tolist()]
    )
    f_cat = st.selectbox("Categoria", cats)
with c2:
    names = ["(todos)"] + sorted([n for n in sales["name"].dropna().unique().tolist()])
    f_name = st.selectbox("Procedimento", names)
with c3:
    f_prof = st.text_input("Profissional contém…")

f = sales.copy()
if f_cat != "(todas)":
    f = f[f["category"] == f_cat]
if f_name != "(todos)":
    f = f[f["name"] == f_name]
if f_prof:
    f = f[f["professional"].str.contains(f_prof, case=False, na=False)]

# KPIs
colA, colB, colC, colD = st.columns(4)
colA.metric("Receita", f"R$ {f['sale_price'].sum():,.2f}")
colB.metric("Comissão", f"R$ {f['commission_value'].sum():,.2f}")
colC.metric("Lucro Líquido", f"R$ {f['net_profit'].sum():,.2f}")
margin_media = (
    (f["net_profit"].sum() / f["sale_price"].sum() * 100)
    if f["sale_price"].sum() > 0
    else 0
)
colD.metric("Margem Média", f"{margin_media:,.2f}%")

st.divider()

# Tabelas
st.subheader("Vendas (detalhe)")
st.dataframe(
    f.sort_values("sale_datetime", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.subheader("Top por Margem (procedimento)")
rank = f.groupby(["name", "category"], as_index=False)[
    ["sale_price", "net_profit"]
].sum()
if not rank.empty:
    rank["margin_%"] = (rank["net_profit"] / rank["sale_price"]) * 100
    rank = rank.sort_values("margin_%", ascending=False).head(20)
    st.dataframe(rank, use_container_width=True, hide_index=True)
else:
    st.info("Sem dados para ranking.")

st.subheader("Receita e Margem por Categoria")
by_cat = f.groupby("category", as_index=False)[["sale_price", "net_profit"]].sum()
if not by_cat.empty:
    by_cat["margin_%"] = (by_cat["net_profit"] / by_cat["sale_price"]) * 100
    st.dataframe(by_cat, use_container_width=True, hide_index=True)
else:
    st.info("Sem dados por categoria.")

st.caption(
    "Fonte: data/silver/fact_procedure_sale/sales.parquet e data/silver/fact_procedure/catalog.parquet"
)
