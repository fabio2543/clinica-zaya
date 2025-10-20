import streamlit as st

st.set_page_config(page_title="Zaya • Financeiro", layout="wide")
st.title("Zaya • Financeiro")
st.caption(
    "Use o menu **Pages** (barra lateral) para navegar entre: Custos Fixos, Produtos e Procedimentos."
)

st.markdown("### Seções disponíveis")
st.markdown("- 🧾 Custos Fixos")
st.markdown("- 🧪 Produtos (compras/entradas)")
st.markdown("- 🧮 Procedimentos (em breve)")

st.info(
    "Dica: mantenha cada página em um arquivo separado para evitar efeitos colaterais durante manutenção."
)
