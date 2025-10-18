import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Zaya • Custos", page_icon="✨", layout="wide")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

st.title("✨ Zaya • Centro de Custos")
st.caption(
    "Organize e padronize os custos da clínica: fixos, produtos e procedimentos."
)

st.markdown("---")
st.subheader("O que você quer fazer agora?")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🧾 Custos Fixos")
    st.write(
        "Inclua/importe aluguel, energia, internet, contabilidade, folha fixa etc."
    )
    st.page_link("pages/10_Custos_Fixos.py", label="Ir para Custos Fixos →", icon="➡️")

with col2:
    st.markdown("### 🧪 Custo dos Produtos")
    st.write("Cadastre insumos e produtos (preço de compra, unidade, marca).")
    st.page_link("pages/20_Custo_Produtos.py", label="Ir para Produtos →", icon="➡️")

with col3:
    st.markdown("### 🧮 Custo dos Procedimentos")
    st.write(
        "Monte a ficha técnica: tempo, consumo de insumos e custo por procedimento."
    )
    st.page_link(
        "pages/30_Custo_Procedimentos.py", label="Ir para Procedimentos →", icon="➡️"
    )

st.markdown("---")
st.info(
    "Dica: use a pasta **`data/`** para guardar seus arquivos. "
    "Você pode importar CSV/XLS/XLSX e o sistema gera o layout padrão."
)
