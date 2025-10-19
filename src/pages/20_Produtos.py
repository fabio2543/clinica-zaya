
import streamlit as st
import pandas as pd
from datetime import datetime, date

from src.dataio.products import (
    read_products,
    upsert_products,
    create_product_purchase,
    update_product_purchase,
    delete_product_purchase,
    list_categories as list_categories_pd,
    list_vendors as list_vendors_pd,
)

st.title("🧪 Produtos (Compras/Entradas)")

# -------------------- FILTROS (Data/Produto + Categoria/Fornecedor) --------------------
row1 = st.columns([1, 1, 1, 1])
start_date = row1[0].date_input("Data inicial", value=date.today().replace(day=1), key="prod_start")
end_date = row1[1].date_input("Data final", value=date.today(), key="prod_end")
product_search = row1[2].text_input("Produto (nome contém)", key="prod_search")
buscar_prod = row1[3].button("🔍 Buscar", use_container_width=True, key="prod_buscar_btn")

row2 = st.columns([1, 1])
cat_opts = ["(todas)"] + list_categories_pd()
category = row2[0].selectbox("Categoria", cat_opts, index=0, key="prod_categoria_filter")
vendor_opts = ["(todos)"] + list_vendors_pd()
vendor = row2[1].selectbox("Fornecedor", vendor_opts, index=0, key="prod_vendor_filter")

st.divider()

# -------------------- LISTAGEM --------------------
dfp = pd.DataFrame()
if buscar_prod:
    tmp = read_products(None if category == "(todas)" else category)
    if vendor != "(todos)" and "vendor_name" in tmp.columns:
        tmp = tmp[tmp["vendor_name"] == vendor]
    if product_search.strip() and "product_name" in tmp.columns:
        s = product_search.strip().lower()
        tmp = tmp[tmp["product_name"].astype(str).str.lower().str.contains(s)]
    if "purchase_date" in tmp.columns:
        tmp["purchase_date"] = pd.to_datetime(tmp["purchase_date"], errors="coerce")
        mask = (tmp["purchase_date"].dt.date >= start_date) & (tmp["purchase_date"].dt.date <= end_date)
        tmp = tmp[mask]
    st.session_state["df_produtos"] = tmp

dfp = st.session_state.get("df_produtos", pd.DataFrame())

# Carregamento inicial (últimos 60 dias) para evitar tela vazia
if dfp.empty and not buscar_prod:
    tmp = read_products(None if category == "(todas)" else category)
    if "purchase_date" in tmp.columns:
        tmp["purchase_date"] = pd.to_datetime(tmp["purchase_date"], errors="coerce")
        start_default = (pd.Timestamp.today() - pd.Timedelta(days=60)).date()
        end_default = pd.Timestamp.today().date()
        mask = (tmp["purchase_date"].dt.date >= start_default) & (tmp["purchase_date"].dt.date <= end_default)
        tmp = tmp[mask]
    st.session_state["df_produtos"] = tmp
    dfp = tmp

if dfp.empty and buscar_prod:
    st.info("Nenhuma entrada encontrada para os filtros atuais.")
elif not dfp.empty:
    st.caption(f"{len(dfp)} registros encontrados")
    dfp_view = dfp.copy()
    dfp_view["Selecionar"] = False

    edited_p = st.data_editor(
        dfp_view,
        column_config={"Selecionar": st.column_config.CheckboxColumn(required=False)},
        hide_index=True,
        use_container_width=True,
        num_rows="dynamic",
        key="grid_produtos",
    )

    selected_ids_p = edited_p.loc[edited_p["Selecionar"], "product_purchase_id"].tolist()

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🗑️ Excluir selecionados", disabled=len(selected_ids_p) == 0, use_container_width=True, key="prod_del_btn"):
            n = delete_product_purchase(selected_ids_p)
            if "df_produtos" in st.session_state:
                df_local = st.session_state["df_produtos"]
                st.session_state["df_produtos"] = df_local[~df_local["product_purchase_id"].isin(selected_ids_p)].reset_index(drop=True)
            st.success(f"🗑️ {n} registro(s) excluído(s) com sucesso.")
            st.rerun()

    with c2:
        if st.button("⬇️ Exportar CSV", use_container_width=True, key="prod_export_btn"):
            csv_bytes = dfp.to_csv(index=False).encode("utf-8")
            st.download_button("Baixar CSV filtrado", data=csv_bytes, file_name=f"produtos_{datetime.now():%Y%m%d}.csv", mime="text/csv")

st.divider()

# -------------------- CADASTRO UNITÁRIO --------------------
st.markdown("### ➕ Entrada de produto (unitária)")
with st.form("form_novo_produto"):
    c1, c2, c3 = st.columns(3)
    p_name = c1.text_input("Nome do produto *", placeholder="Ex.: Luvas Nitrílicas (M)")
    unit_price = c2.number_input("Preço unitário (R$) *", min_value=0.0, step=0.01, format="%.2f")
    quantity = c3.number_input("Quantidade *", min_value=0, step=1)
    c4, c5, c6 = st.columns(3)
    purchase_date = c4.date_input("Data da compra *", value=date.today())
    sku = c5.text_input("SKU", placeholder="Opcional")
    category_new = c6.selectbox("Categoria", options=list_categories_pd() + ["Não informado"], key="prod_categoria_form")
    c7, c8 = st.columns(2)
    vendor_new = c7.selectbox("Fornecedor", options=list_vendors_pd() + ["Não informado"], key="prod_vendor_form")
    notes = c8.text_input("Observações")
    submit_p = st.form_submit_button("Salvar")

    if submit_p:
        row = {
            "product_name": p_name,
            "unit_price": unit_price,
            "purchase_date": purchase_date.isoformat(),
            "quantity": quantity,
            "sku": sku,
            "category": category_new,
            "vendor": vendor_new,
            "notes": notes,
        }
        res = create_product_purchase(row)
        st.success(f"Entrada registrada. {res}")

st.divider()

# -------------------- TEMPLATE + CARGA --------------------
st.markdown("### 📥 Importação (CSV/XLSX) + Template")

sample_p = pd.DataFrame([{
    "product_name": "Luvas Nitrílicas (M)",
    "unit_price": 0.80,
    "purchase_date": datetime.now().date().isoformat(),
    "quantity": 300,
    "sku": "LUV-NIT-M",
    "category": "Insumos",
    "vendor": "Fornecedor Geral",
    "notes": "Caixa com 100",
}])
st.download_button("⬇️ Baixar template (CSV)", data=sample_p.to_csv(index=False).encode("utf-8"), file_name="template_produtos.csv", mime="text/csv")

up_p = st.file_uploader("Envie CSV/XLSX no layout padrão de produtos", type=["csv", "xls", "xlsx"], key="upl_prod")
if up_p:
    try:
        raw_p = pd.read_csv(up_p) if up_p.name.lower().endswith(".csv") else pd.read_excel(up_p)
        st.write("Prévia do arquivo:")
        st.dataframe(raw_p.head(50), use_container_width=True)

        required_p = {"product_name", "unit_price", "purchase_date", "quantity"}
        norm_cols_p = {c.strip().lower() for c in raw_p.columns}
        missing_p = required_p - norm_cols_p

        if missing_p:
            st.error(f"Colunas obrigatórias ausentes: {', '.join(sorted(missing_p))}")
        else:
            if st.button("Carregar arquivo de produtos", type="primary"):
                res = upsert_products(raw_p)
                st.success(f"Carga concluída. Inseridos: {res['inserted']} | Atualizados: {res['updated']} | Erros: {res['errors']}")
    except Exception as e:
        st.error(f"Erro ao processar arquivo: {e}")
