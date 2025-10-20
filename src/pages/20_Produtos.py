import streamlit as st
import pandas as pd
from datetime import date
from pathlib import Path
import inspect

# =============================================================================
# IMPORTS DO DATA LAYER (PRINCIPAIS)
# =============================================================================
from src.dataio.products import (
    upsert_products,
    create_product_purchase,
    delete_product_purchase,
)

# =============================================================================
# FALLBACKS (para funções que podem não existir no módulo products.py)
# =============================================================================
# --- list_categories ---
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


# --- list_vendors ---
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


# --- read_products ---
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


# =============================================================================
# HELPERS
# =============================================================================
def _ensure_flag(df, col: str, default: bool = False):
    """Garante que a coluna booleana exista e esteja normalizada."""
    if df is None or df.empty:
        return df
    if col not in df.columns:
        df[col] = default
    else:
        df[col] = df[col].fillna(False).astype(bool)
    return df


def _pick_first_col(
    df: pd.DataFrame, candidates: list[str] | tuple[str, ...]
) -> str | None:
    """Retorna a primeira coluna existente dentre as candidatas."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _get_value(row: pd.Series, candidates: list[str] | tuple[str, ...], default=None):
    """Pega o primeiro valor existente em `row` dado um conjunto de nomes de coluna."""
    for c in candidates:
        if c in row.index and pd.notna(row[c]):
            return row[c]
    return default


# =============================================================================
# ADAPTERS (compatibilidade de assinatura do data layer)
# =============================================================================
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
    Suporta tanto `row` quanto kwargs. Monta payload PT+EN.
    """
    sig = inspect.signature(create_product_purchase)
    allowed = set(sig.parameters.keys())

    # Payload "row" completo, com chaves PT e EN
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
        "unit_price": valor_compra,
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

    # fallback: kwargs individuais
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
    Suporta tanto `row` quanto kwargs. Monta payload PT+EN.
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


def _call_delete_product_purchase_adapter(row) -> None:
    """
    Deleção resiliente:
    - Detecta a coluna correta de ID
    - Normaliza para lista (ids)
    - Chama delete_product_purchase com ids/id/row conforme assinatura
    """
    # linha como dict
    if isinstance(row, pd.Series):
        r = row.to_dict()
    elif isinstance(row, dict):
        r = row
    else:
        r = {}

    # descobrir a coluna de id
    id_value = None
    for c in ["product_purchase_id", "id", "purchase_id"]:
        if c in r and pd.notna(r[c]):
            id_value = r[c]
            break

    # normalizar para lista de strings
    ids = []
    if isinstance(id_value, (list, tuple, set)):
        ids = [str(x) for x in id_value if pd.notna(x)]
    elif id_value is not None and id_value != "":
        ids = [str(id_value)]

    # assinatura da função
    sig = inspect.signature(delete_product_purchase)
    allowed = set(sig.parameters.keys())

    if "ids" in allowed:
        return delete_product_purchase(ids=ids)
    if "id" in allowed:
        return delete_product_purchase(id=ids[0] if ids else None)
    if "row" in allowed:
        return delete_product_purchase(row=r)

    # fallback: tentar posicional com lista
    return delete_product_purchase(ids)


# =============================================================================
# UI
# =============================================================================
st.set_page_config(page_title="Produtos | Zaya", layout="wide")
st.title("🧪 Produtos (Compras/Entradas)")

menu = st.radio(
    "Selecione a operação",
    ["Visualizar", "Cadastro", "Atualização", "Exclusão"],
    horizontal=True,
)

# carregar dataset
df = read_products(None)
if df is None or df.empty:
    st.warning("Nenhum produto encontrado ainda. Faça um cadastro.")
    df = pd.DataFrame(
        columns=["id", "nome", "categoria", "valor_compra", "quantidade", "is_deleted"]
    )

# normalizar flag
df = _ensure_flag(df, "is_deleted", False)

# =============================================================================
# VISUALIZAÇÃO
# =============================================================================
if menu == "Visualizar":
    st.subheader("📦 Catálogo de Produtos / Insumos")
    hide_deleted = st.checkbox("Ocultar itens excluídos", value=True)
    if hide_deleted:
        df = df[~df["is_deleted"]].copy()
    st.dataframe(df, use_container_width=True, hide_index=True)

# =============================================================================
# CADASTRO
# =============================================================================
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
            try:
                _call_create_product_purchase_adapter(
                    nome=str(nome).strip(),
                    categoria=str(categoria).strip(),
                    fornecedor=str(fornecedor).strip(),
                    valor_compra=float(valor_compra or 0.0),
                    quantidade=float(quantidade or 0.0),
                    data_compra=(
                        data_compra.isoformat()
                        if hasattr(data_compra, "isoformat")
                        else str(data_compra)
                    ),
                )
                st.toast(f"Produto '{nome}' cadastrado.", icon="✅")
                st.success(f"Produto '{nome}' cadastrado com sucesso.")
            except Exception as e:
                st.error("Erro ao salvar o produto.")
                st.exception(e)

# =============================================================================
# ATUALIZAÇÃO
# =============================================================================
elif menu == "Atualização":
    st.subheader("✏️ Atualizar produto existente")
    base = df[~df["is_deleted"]].copy()
    if base.empty:
        st.info("Não há produtos para atualizar.")
    else:
        # escolher coluna de exibição dinamicamente
        name_col = _pick_first_col(base, ["nome", "name", "product_name", "title"])
        if not name_col:
            label_col = "__label__"
            if "id" in base.columns:
                base[label_col] = base["id"].astype(str)
            else:
                base[label_col] = base.index.astype(str)
        else:
            label_col = name_col

        opcoes = base[label_col].astype(str).tolist()
        selecionado = st.selectbox("Selecione o produto", opcoes)
        linha = base.loc[base[label_col].astype(str) == str(selecionado)].iloc[0]

        # detectar colunas
        cat_col = _pick_first_col(base, ["categoria", "category"])
        price_cols = ["valor_compra", "purchase_price", "unit_cost", "cost", "price"]
        qty_cols = ["quantidade", "quantity", "qty"]

        # valores padrão (respeitando esquema atual)
        nome_padrao = _get_value(linha, [name_col] if name_col else [], "")
        categoria_padrao = _get_value(linha, [cat_col] if cat_col else [], "")

        preco_padrao = _get_value(linha, price_cols, 0.0)
        try:
            preco_padrao = float(preco_padrao or 0.0)
        except Exception:
            preco_padrao = 0.0

        qtd_padrao = _get_value(linha, qty_cols, 0.0)
        try:
            qtd_padrao = float(qtd_padrao or 0.0)
        except Exception:
            qtd_padrao = 0.0

        # UI com defaults corretos
        nome = st.text_input("Nome do produto", str(nome_padrao))

        categorias_opts = list_categories_pd()
        idx_cat = 0
        if categoria_padrao and categoria_padrao in categorias_opts:
            idx_cat = categorias_opts.index(categoria_padrao)
        categoria = st.selectbox("Categoria", categorias_opts, index=idx_cat)

        valor_compra = st.number_input(
            "Valor de compra (R$)", value=preco_padrao, step=0.01, min_value=0.0
        )
        quantidade = st.number_input(
            "Quantidade", value=qtd_padrao, step=1.0, min_value=0.0
        )

        col_u, col_d = st.columns(2)
        with col_u:
            if st.button("Salvar alterações", type="primary", use_container_width=True):
                try:
                    _call_upsert_products_adapter(
                        id=linha.get("id"),
                        nome=str(nome).strip(),
                        categoria=str(categoria).strip(),
                        valor_compra=float(valor_compra or 0.0),
                        quantidade=float(quantidade or 0.0),
                    )
                    exibido = str(nome).strip() or str(selecionado)
                    st.toast("Alterações salvas.", icon="✅")
                    st.success(f"Produto '{exibido}' atualizado com sucesso.")
                except Exception as e:
                    st.error("Erro ao atualizar o produto.")
                    st.exception(e)

        with col_d:
            if st.button("Excluir este produto", use_container_width=True):
                try:
                    _call_delete_product_purchase_adapter(linha)
                    exibido = (
                        str(linha.get(name_col, selecionado))
                        if name_col
                        else str(selecionado)
                    )
                    st.toast("Produto excluído (soft delete).", icon="🗑️")
                    st.success(
                        f"Produto '{exibido}' excluído com sucesso (soft delete)."
                    )
                except Exception as e:
                    st.error("Erro ao excluir o produto.")
                    st.exception(e)

# =============================================================================
# EXCLUSÃO
# =============================================================================
elif menu == "Exclusão":
    st.subheader("🗑️ Exclusão (soft delete)")
    base = df[~df["is_deleted"]].copy()
    if base.empty:
        st.info("Não há produtos para excluir.")
    else:
        name_col = _pick_first_col(base, ["nome", "name", "product_name", "title"])
        if not name_col:
            label_col = "__label__"
            if "id" in base.columns:
                base[label_col] = base["id"].astype(str)
            else:
                base[label_col] = base.index.astype(str)
        else:
            label_col = name_col

        opcoes = base[label_col].astype(str).tolist()
        selecionado = st.selectbox("Selecione o produto", opcoes)
        linha = base.loc[base[label_col].astype(str) == str(selecionado)].iloc[0]

        if st.button("Excluir produto", type="primary", use_container_width=True):
            try:
                _call_delete_product_purchase_adapter(linha)
                exibido = (
                    str(linha.get(name_col, selecionado))
                    if name_col
                    else str(selecionado)
                )
                st.toast("Produto excluído (soft delete).", icon="🗑️")
                st.success(f"Produto '{exibido}' excluído com sucesso (soft delete).")
            except Exception as e:
                st.error("Erro ao excluir o produto.")
                st.exception(e)
