import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Zaya • Plataforma", page_icon="✨", layout="wide")

# === HEADER ===
st.title("✨ Zaya • Plataforma Operacional")
st.caption(
    "Bem-vindo à central de controle da Clínica Zaya. Escolha um módulo para começar."
)

st.markdown("---")
st.subheader("📂 Módulos do Sistema")

col1, col2, col3 = st.columns(3)

# === FINANCEIRO ===
with col1:
    st.markdown("### 💰 Financeiro")
    st.write(
        "Controle e análise de custos fixos, produtos e procedimentos. "
        "Aqui você organiza o núcleo financeiro da clínica."
    )
    st.page_link("pages/10_Financeiro.py", label="Abrir módulo Financeiro →", icon="➡️")

# === RELATÓRIOS ===
with col2:
    st.markdown("### 📊 Relatórios")
    st.write(
        "Painéis com indicadores e métricas estratégicas da operação. "
        "Acompanhe desempenho, rentabilidade e evolução de custos."
    )
    st.page_link("pages/20_Relatorios.py", label="Abrir módulo Relatórios →", icon="➡️")

# === CONFIGURAÇÕES ===
with col3:
    st.markdown("### ⚙️ Configurações")
    st.write(
        "Personalize parâmetros do sistema, usuários e variáveis operacionais. "
        "Ideal para ajustes de cadastros e preferências."
    )
    st.page_link(
        "pages/30_Configuracoes.py", label="Abrir módulo Configurações →", icon="➡️"
    )

st.markdown("---")
st.info(
    "💡 Use o menu lateral para navegar entre módulos. "
    "A pasta **`data/`** armazena os arquivos e bases de dados locais do sistema."
)
