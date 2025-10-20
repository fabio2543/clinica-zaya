import streamlit as st

st.set_page_config(page_title="Configurações | Zaya", layout="wide")
st.title("⚙️ Configurações")

st.subheader("Comissão padrão")
commission_model = st.selectbox("Modelo", ["percent", "fixed", "tiered"], index=0)
commission_rate = st.number_input(
    "% Comissão (0-1)", min_value=0.0, max_value=1.0, step=0.01, value=0.30
)
commission_fixed = st.number_input(
    "Comissão fixa (R$)", min_value=0.0, step=1.0, value=0.0
)
tiers = st.text_area(
    "Tiers (JSON)",
    placeholder='[{"min":0,"max":500,"rate":0.2},{"min":500,"max":999999,"rate":0.3}]',
)
st.info(
    "(Opcional) Persistir esses valores em storage/config quando integrar o backend."
)
