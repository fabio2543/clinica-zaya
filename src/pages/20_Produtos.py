import streamlit as st
import pandas as pd
from datetime import date
from pathlib import Path
import inspect

from src.dataio.products import (
    upsert_products,
    create_product_purchase,
    delete_product_purchase,
)

try:
    from src.dataio.products import list_categories as list_categories_pd
except (ImportError, AttributeError):

    def list_categories_pd() -> list[str]:
        dim_path = Path("data/silver/dim_category.parquet")
        fact_path = Path("data/silver/fact_product/products.parquet")
        if dim_path.exists():
            df = pd.read_parquet(dim_path)
            if not df.empty and "category" in df.columns:
                return sorted(df["category"].dropna().astype(str).unique().tolist())
        if fact_path.exists():
            df = pd.read_parquet(fact_path)
            if not df.empty and "category" in df.columns:
                return sorted(df["category"].dropna().astype(str).unique().tolist())
        return ["produto"]


try:
    from src.dataio.products import list_vendors as list_vendors_pd
except (ImportError, AttributeError):

    def list_vendors_pd() -> list[str]:
        purchases_path = Path("data/silver/fact_product_purchase/purchases.parquet")
        alt_path = Path("data/silver/fact_product/products.parquet")
        for path in (purchases_path, alt_path):
            if path.exists():
                df = pd.read_parquet(path)
                for col in ("vendor", "vendor_name", "fornecedor"):
                    if col in df.columns:
                        vals = (
                            df[col]
                            .dropna()
                            .astype(str)
                            .replace({"": None})
                            .dropna()
                            .unique()
                            .tolist()
                        )
                        return sorted(set(vals))
        return []


try:
    from src.dataio.products import read_products
except (ImportError, AttributeError):

    def read_products(_: None) -> pd.DataFrame:
        purchases_path = Path("data/silver/fact_product_purchase/purchases.parquet")
        alt_path = Path("data/silver/fact_product/products.parquet")
        if purchases_path.exists():
            return pd.read_parquet(purchases_path)
        if alt_path.exists():
            return pd.read_parquet(alt_path)
        return pd.DataFrame()


def _call_create_product_purchase_adapter(
    *,
    nome: str,
    categoria: str,
    fornecedor: str,
    valor_compra: float,
    quantidade: float,
    data_compra,
):
    """
    Se create_product_purchase aceitar apenas `row`, montamos o payload completo em PT+EN.
    """

    sig = inspect.signature(create_product_purchase)
    allowed = set(sig.parameters.keys())

    # Payload "row" com chaves em PT e EN para máxima compatibilidade
    row_payload = {
        # Identificação
        "nome": nome,
        "name": nome,
        "product_name": nome,
        "title": nome,
        # Categoria
        "categoria": categoria,
        "category": categoria,
        # Fornecedor
        "fornecedor": fornecedor,
        "vendor": fornecedor,
        "supplier": fornecedor,
        # Preço / custo
        "valor_compra": valor_compra,
        "purchase_price": valor_compra,
        "unit_cost": valor_compra,
        "cost": valor_compra,
        "price": valor_compra,
        # Quantidade
        "quantidade": quantidade,
        "quantity": quantidade,
        "qty": quantidade,
        # Datas
        "data_compra": str(data_compra),
        "purchase_date": str(data_compra),
        "date": str(data_compra),
        "dt": str(data_compra),
    }

    if "row" in allowed:
        return create_product_purchase(row=row_payload)

    # fallback: se não tiver `row`, tenta mapear por kwargs individuais (caso exista outro formato)
    candidates = {
        "name": nome,
        "product_name": nome,
        "title": nome,
        "category": categoria,
        "vendor": fornecedor,
        "supplier": fornecedor,
        "purchase_price": valor_compra,
        "unit_cost": valor_compra,
        "cost": valor_compra,
        "price": valor_compra,
        "quantity": quantidade,
        "qty": quantidade,
        "purchase_date": data_compra,
        "date": data_compra,
        "dt": data_compra,
    }
    payload = {k: v for k, v in candidates.items() if k in allowed}
    if not payload:
        raise TypeError(
            f"create_product_purchase sem match. Aceitos: {sorted(allowed)}"
        )
    return create_product_purchase(**payload)


def _call_create_product_purchase_adapter(
    *,
    nome: str,
    categoria: str,
    fornecedor: str,
    valor_compra: float,
    quantidade: float,
    data_compra,
):
    """
    Se create_product_purchase aceitar apenas `row`, montamos o payload completo em PT+EN.
    """

    sig = inspect.signature(create_product_purchase)
    allowed = set(sig.parameters.keys())

    # Payload "row" com chaves em PT e EN para máxima compatibilidade
    row_payload = {
        # Identificação
        "nome": nome,
        "name": nome,
        "product_name": nome,
        "title": nome,
        # Categoria
        "categoria": categoria,
        "category": categoria,
        # Fornecedor
        "fornecedor": fornecedor,
        "vendor": fornecedor,
        "supplier": fornecedor,
        # Preço / custo
        "valor_compra": valor_compra,
        "purchase_price": valor_compra,
        "unit_cost": valor_compra,
        "cost": valor_compra,
        "price": valor_compra,
        # Quantidade
        "quantidade": quantidade,
        "quantity": quantidade,
        "qty": quantidade,
        # Datas
        "data_compra": str(data_compra),
        "purchase_date": str(data_compra),
        "date": str(data_compra),
        "dt": str(data_compra),
    }

    if "row" in allowed:
        return create_product_purchase(row=row_payload)

    # fallback: se não tiver `row`, tenta mapear por kwargs individuais (caso exista outro formato)
    candidates = {
        "name": nome,
        "product_name": nome,
        "title": nome,
        "category": categoria,
        "vendor": fornecedor,
        "supplier": fornecedor,
        "purchase_price": valor_compra,
        "unit_cost": valor_compra,
        "cost": valor_compra,
        "price": valor_compra,
        "quantity": quantidade,
        "qty": quantidade,
        "purchase_date": data_compra,
        "date": data_compra,
        "dt": data_compra,
    }
    payload = {k: v for k, v in candidates.items() if k in allowed}
    if not payload:
        raise TypeError(
            f"create_product_purchase sem match. Aceitos: {sorted(allowed)}"
        )
    return create_product_purchase(**payload)


def _call_upsert_products_adapter(*, id, nome, categoria, valor_compra, quantidade):
    """
    Suporta tanto `row` quanto kwargs. Usa PT+EN no payload.
    """

    sig = inspect.signature(upsert_products)
    allowed = set(sig.parameters.keys())

    row_payload = {
        "id": id,
        "nome": nome,
        "name": nome,
        "product_name": nome,
        "categoria": categoria,
        "category": categoria,
        "valor_compra": valor_compra,
        "purchase_price": valor_compra,
        "unit_cost": valor_compra,
        "cost": valor_compra,
        "quantidade": quantidade,
        "quantity": quantidade,
        "qty": quantidade,
    }

    if "row" in allowed:
        return upsert_products(row=row_payload)

    candidates = {
        "id": id,
        "name": nome,
        "product_name": nome,
        "category": categoria,
        "purchase_price": valor_compra,
        "unit_cost": valor_compra,
        "cost": valor_compra,
        "quantity": quantidade,
        "qty": quantidade,
    }
    payload = {k: v for k, v in candidates.items() if k in allowed}
    if not payload:
        raise TypeError(f"upsert_products sem match. Aceitos: {sorted(allowed)}")
    return upsert_products(**payload)


st.set_page_config(page_title="Produtos | Zaya", layout="wide")
st.title("🧪 Produtos (Compras/Entradas)")

menu = st.radio(
    "Selecione a operação",
    ["Visualizar", "Cadastro", "Atualização", "Exclusão"],
    horizontal=True,
)

df = read_products(None)
if df.empty:
    st.warning("Nenhum produto encontrado ainda. Faça um cadastro.")
    df = pd.DataFrame(
        columns=["id", "nome", "categoria", "valor_compra", "quantidade", "is_deleted"]
    )

if menu == "Visualizar":
    st.subheader("📦 Catálogo de Produtos / Insumos")
    hide_deleted = st.checkbox("Ocultar itens excluídos", value=True)
    if hide_deleted and "is_deleted" in df.columns:
        df = df[~df["is_deleted"].fillna(False)]
    st.dataframe(df, use_container_width=True, hide_index=True)

elif menu == "Cadastro":
    st.subheader("🆕 Novo Produto / Insumo")
    col1, col2, col3 = st.columns(3)
    with col1:
        nome = st.text_input("Nome do produto")
        categoria = st.selectbox("Categoria", list_categories_pd())
    with col2:
        valor_compra = st.number_input(
            "Valor de compra (R$)", min_value=0.0, step=0.01, value=0.0
        )
        quantidade = st.number_input("Quantidade", min_value=0.0, step=1.0, value=1.0)
    with col3:
        fornecedor = st.selectbox("Fornecedor", list_vendors_pd())
        data_compra = st.date_input("Data da compra", value=date.today())

    if st.button("Salvar produto", type="primary", use_container_width=True):
        if not nome:
            st.error("Informe o nome do produto.")
        else:
            _call_create_product_purchase_adapter(
                nome=nome,
                categoria=categoria,
                fornecedor=fornecedor,
                valor_compra=valor_compra,
                quantidade=quantidade,
                data_compra=data_compra,
            )
            st.success(f"Produto '{nome}' cadastrado com sucesso.")
            st.experimental_rerun()

elif menu == "Atualização":
    st.subheader("✏️ Atualizar produto existente")
    base = df[~df["is_deleted"].fillna(False)].copy()
    if base.empty:
        st.info("Não há produtos para atualizar.")
    else:
        selecionado = st.selectbox("Selecione o produto", base["nome"].tolist())
        linha = base[base["nome"] == selecionado].iloc[0]

        nome = st.text_input("Nome do produto", linha.get("nome", ""))
        categoria = st.selectbox("Categoria", list_categories_pd(), index=0)
        valor_compra = st.number_input(
            "Valor de compra (R$)", value=float(linha.get("valor_compra", 0.0))
        )
        quantidade = st.number_input(
            "Quantidade", value=float(linha.get("quantidade", 0.0))
        )

        if st.button("Salvar alterações", type="primary", use_container_width=True):
            _call_upsert_products_adapter(
                id=linha.get("id"),
                nome=nome,
                categoria=categoria,
                valor_compra=valor_compra,
                quantidade=quantidade,
            )
            st.success("Produto atualizado com sucesso.")
            st.experimental_rerun()

elif menu == "Exclusão":
    st.subheader("🗑️ Exclusão (soft delete)")
    base = df[~df["is_deleted"].fillna(False)].copy()
    if base.empty:
        st.info("Não há produtos para excluir.")
    else:
        selecionado = st.selectbox("Selecione o produto", base["nome"].tolist())
        linha = base[base["nome"] == selecionado].iloc[0]
        if st.button("Excluir produto", type="primary", use_container_width=True):
            delete_product_purchase(linha.get("id"))
            st.success("Produto excluído com sucesso (soft delete).")
            st.experimental_rerun()
