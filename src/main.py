import streamlit as st

st.set_page_config(page_title="Zaya • Plataforma", page_icon="✨", layout="wide")

st.title("✨ Zaya • Plataforma Operacional")
st.caption("Bem-vindo à central da Clínica Zaya. Escolha um módulo para começar.")

st.markdown("---")
st.subheader("📂 Módulos do Sistema")

# ➜ Cria as colunas ANTES de usá-las
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 💰 Financeiro")
    st.write("Custos fixos, produtos e procedimentos em um único módulo.")
    st.page_link("pages/10_Financeiro.py", label="Abrir módulo Financeiro →", icon="➡️")

with col2:
    st.markdown("### 📊 Relatórios")
    st.write("Painéis com KPIs e análises financeiras.")
    st.page_link("pages/20_Relatorios.py", label="Abrir módulo Relatórios →", icon="➡️")

st.markdown("---")
st.info(
    "💡 Use o menu lateral para navegar. A pasta **`data/`** guarda seus arquivos locais."
)
