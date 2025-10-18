
import streamlit as st
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Zaya Starter", page_icon="✨", layout="wide")

st.title("🚀 Zaya Streamlit + Docker Starter")
st.caption("Base pronta para desenvolvimento com Python, Streamlit e Docker.")

st.sidebar.header("Configurações")
app_env = os.getenv("APP_ENV", "dev")
st.sidebar.write("Ambiente:", f"`{app_env}`")

uploaded = st.sidebar.file_uploader("Suba uma planilha (.xlsx) para visualizar", type=["xlsx"])

if uploaded:
    try:
        df = pd.read_excel(uploaded)
        st.success("Arquivo carregado com sucesso!")
        st.dataframe(df.head(100), use_container_width=True)
    except Exception as e:
        st.error(f"Falha ao ler o arquivo: {e}")

st.markdown("---")
st.subheader("Como usar")
st.markdown("""
- Use **Docker** para rodar a aplicação sem depender do ambiente local.
- Edite os arquivos em `src/` e o app recarrega automaticamente.
- Configure variáveis em `.env`.
""")
