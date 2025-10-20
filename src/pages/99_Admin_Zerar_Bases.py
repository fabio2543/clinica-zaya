import streamlit as st
from pathlib import Path
import shutil

st.title("🧹 Administração • Zerar Bases")

st.warning("Cuidado! Esta ação apaga dados do data lake. Faça backup antes.")

DATA = Path("data")
SILVER = DATA / "silver"

FACTS = [
    "fact_fixed_cost",
    "fact_product_purchase",
]

DIMS = [
    "dim_category",
    "dim_vendor",
    "dim_cost_center",
    "dim_payment_method",
]

st.subheader("Seleção")
col1, col2 = st.columns(2)
wipe_fixed = col1.checkbox("Apagar Custos Fixos (fact_fixed_cost)", value=True)
wipe_prod = col1.checkbox("Apagar Produtos (fact_product_purchase)", value=True)
wipe_dims = col2.checkbox(
    "Apagar Dimensões (categorias, fornecedores, etc.)",
    value=False,
    help="Será recriado via get_or_create, mas você perderá rótulos/ativo/inativo.",
)

st.divider()
if st.button("🔥 Apagar selecionados", type="primary", use_container_width=True):
    removed = []

    def _rm_tree(p: Path):
        if p.exists():
            shutil.rmtree(p)
            removed.append(str(p))

    if wipe_fixed:
        _rm_tree(SILVER / "fact_fixed_cost")
    if wipe_prod:
        _rm_tree(SILVER / "fact_product_purchase")
    if wipe_dims:
        for d in DIMS:
            f = SILVER / f"{d}.parquet"
            if f.exists():
                f.unlink()
                removed.append(str(f))

    if removed:
        st.success("Remoção concluída:")
        for r in removed:
            st.write(f"- {r}")
    else:
        st.info("Nada foi removido (verifique seleções).")
