import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Custo dos Procedimentos", layout="wide")
st.title("🧮 Custo dos Procedimentos")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

st.markdown("#### Estrutura sugerida")
st.code(
    """procedimento, duracao_min, profissional, custo_hora_profissional,
item_1_sku, item_1_qtd, item_2_sku, item_2_qtd, ..., custos_fixos_rateio_opcional""",
    language="text",
)

st.sidebar.header("Importar ficha técnica (CSV/XLSX)")
up = st.sidebar.file_uploader("Arquivo", type=["csv", "xls", "xlsx"])

if up:
    try:
        df = pd.read_csv(up) if up.name.endswith(".csv") else pd.read_excel(up)
        st.success(f"Arquivo lido com {len(df)} registros.")
        st.dataframe(df.head(100), use_container_width=True)
    except Exception as e:
        st.error(f"Erro: {e}")

st.markdown("---")
st.subheader("Montar procedimento (exemplo)")
with st.form("novo_proc"):
    c1, c2, c3 = st.columns(3)
    procedimento = c1.text_input("Nome do procedimento")
    duracao = c2.number_input("Duração (min)", min_value=0, step=5)
    custo_hora = c3.number_input(
        "Custo/hora do profissional (R$)", min_value=0.0, step=10.0, format="%.2f"
    )
    st.caption(
        "Em breve: buscar itens cadastrados em Produtos para compor o custo de consumo."
    )
    ok = st.form_submit_button("Calcular (mock)")
    if ok:
        custo_mao_obra = (duracao / 60.0) * custo_hora
        st.info(f"Custo mão de obra estimado: **R$ {custo_mao_obra:,.2f}**")
