import streamlit as st

# Import robusto: tenta sem prefixo (modules) e cai para src.modules
try:
    from modules import procedures  # quando o CWD é src/
except ImportError:
    from src.modules import procedures  # se seu app sobe com CWD no repo e src é pacote

st.set_page_config(page_title="Procedimentos | Zaya", layout="wide")
st.title("🧪 Procedimentos")
procedures.render()
