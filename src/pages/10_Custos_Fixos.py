import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Custos Fixos", layout="wide")
st.title("🧾 Custos Fixos")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

st.sidebar.header("Importação/Exportação")
arquivo = st.sidebar.file_uploader("CSV/XLS/XLSX", type=["csv", "xls", "xlsx"])

st.markdown("#### Layout padrão (obrigatórios)")
st.code("period, date, description, category, amount", language="text")

if arquivo:
    try:
        if arquivo.name.lower().endswith(".csv"):
            df = pd.read_csv(arquivo)
        else:
            df = pd.read_excel(arquivo)
        st.success(f"Arquivo lido com {len(df)} linhas.")
        st.dataframe(df.head(100), use_container_width=True)
    except Exception as e:
        st.error(f"Erro ao ler: {e}")

st.markdown("---")
st.subheader("Incluir lançamento manual")
with st.form("novo_custo_fixo"):
    c1, c2, c3 = st.columns(3)
    period = c1.text_input("Competência (AAAA-MM)", placeholder="2025-10")
    date = c2.date_input("Data")
    category = c3.selectbox(
        "Categoria",
        [
            "Aluguel",
            "Energia",
            "Internet",
            "Pessoal",
            "Contabilidade",
            "Limpeza",
            "Marketing",
            "Softwares",
            "Segurança",
            "Outros",
        ],
    )
    description = st.text_input("Descrição", placeholder="Ex.: Aluguel da sala")
    amount = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f")
    submitted = st.form_submit_button("Adicionar")

    if submitted:
        st.success(
            "Lançamento registrado (mock). Na próxima etapa, persistiremos em `data/custos_fixos.parquet`."
        )
