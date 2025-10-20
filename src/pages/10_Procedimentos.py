import streamlit as st
from src.modules import procedures

st.set_page_config(page_title="Procedimentos | Zaya", layout="wide")
st.title("🧪 Procedimentos")
procedures.render()
