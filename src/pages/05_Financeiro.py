# src/pages/05_Financeiro.py
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Financeiro | Workflow", layout="wide")
st.title("💼 Financeiro — Workflow de Cadastro")


# -----------------------------------------------------------------------------
# Helpers de UI
# -----------------------------------------------------------------------------
def _safe_page_link(rel_path: str, label: str, icon: str = "➡️"):
    """Cria um page_link só se a página existir, senão mostra uma dica."""
    base = Path(__file__).parent  # src/pages
    src_dir = base.parent  # src/
    target = src_dir / rel_path
    if target.exists():
        st.page_link(rel_path, label=label, icon=icon)
    else:
        st.caption(f"🔎 Página não encontrada: `{rel_path}`")


def _exists_any(paths: list[str | Path]) -> Path | None:
    """Retorna o primeiro arquivo existente em paths, ou None."""
    for p in paths:
        p = Path(p)
        if p.exists() and p.is_file():
            return p
    return None


@st.cache_data(show_spinner=False)
def _count_rows(path: str | Path) -> int:
    """Conta linhas de um parquet (retorna 0 se não conseguir)."""
    try:
        df = pd.read_parquet(path)
        return 0 if df is None else len(df)
    except Exception:
        return 0


def _status_badge(ok: bool):
    bg = "✅ Concluído" if ok else "⏳ Pendente"
    color = "green" if ok else "orange"
    st.markdown(
        f"<span style='color:{color};font-weight:600'>{bg}</span>",
        unsafe_allow_html=True,
    )


def _step_card(title: str, subtitle: str):
    st.markdown(f"### {title}")
    st.caption(subtitle)


def _toast_and_success(msg: str, icon: str = "✅"):
    st.toast(msg, icon=icon)
    st.success(msg)


# -----------------------------------------------------------------------------
# Detectores de dados (para validar cada etapa)
# -----------------------------------------------------------------------------
def _has_fixed_costs() -> bool:
    candidates = [
        "data/silver/fixed_costs.parquet",
        "data/silver/fact_fixed_costs/fixed_costs.parquet",
        "data/silver/dim_fixed_costs.parquet",
    ]
    f = _exists_any(candidates)
    return _count_rows(f) > 0 if f else False


def _has_products() -> bool:
    candidates = [
        "data/silver/fact_product_purchase/purchases.parquet",
        "data/silver/fact_product/products.parquet",
    ]
    f = _exists_any(candidates)
    return _count_rows(f) > 0 if f else False


def _has_procedures() -> bool:
    candidates = [
        "data/silver/fact_procedure/catalog.parquet",
    ]
    f = _exists_any(candidates)
    return _count_rows(f) > 0 if f else False


# -----------------------------------------------------------------------------
# Estado do wizard
# -----------------------------------------------------------------------------
if "fin_wizard_step" not in st.session_state:
    st.session_state["fin_wizard_step"] = 1

step = st.session_state["fin_wizard_step"]

# Estado real dos dados no disco
has_step1 = _has_fixed_costs()
has_step2 = _has_products()
has_step3 = _has_procedures()

# Se já houver dados de etapas futuras, permite pular (não trava o usuário)
if has_step1 and step < 2:
    step = 2
if has_step1 and has_step2 and step < 3:
    step = 3
st.session_state["fin_wizard_step"] = step

# -----------------------------------------------------------------------------
# Header do progresso
# -----------------------------------------------------------------------------
cols = st.columns(3)
with cols[0]:
    st.subheader("1) Custos Fixos")
    _status_badge(has_step1)
with cols[1]:
    st.subheader("2) Produtos")
    _status_badge(has_step2)
with cols[2]:
    st.subheader("3) Procedimentos")
    _status_badge(has_step3)

st.divider()

# -----------------------------------------------------------------------------
# Etapas
# -----------------------------------------------------------------------------
# ETAPA 1 – Custos Fixos
if step == 1:
    _step_card(
        "Etapa 1 — Cadastre seus Custos Fixos",
        "Defina aluguel, energia, salários/encargos, softwares e demais despesas recorrentes.",
    )

    # Atalhos para páginas existentes (não quebram se não existirem)
    st.info("Abra a página de Custos Fixos para cadastrar ou importar:")
    _safe_page_link("pages/10_Custos_Fixos.py", "Abrir Custos Fixos →", "🧾")

    # Diagnóstico de dados
    st.markdown("##### Situação dos dados")
    st.write("Concluído?: ", "✅" if has_step1 else "⏳")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button(
            "Marcar etapa como concluída", type="primary", use_container_width=True
        ):
            # Validação suave: se não houver dados ainda, pergunta confirmação
            if not _has_fixed_costs():
                st.warning(
                    "Não detectei dados de Custos Fixos. Se já cadastrou em outra fonte, prossiga."
                )
            st.session_state["fin_wizard_step"] = 2
            _toast_and_success(
                "Etapa 1 marcada como concluída. Vá para Produtos.", "✅"
            )

    with c2:
        if has_step1 and st.button("Pular (já concluído)", use_container_width=True):
            st.session_state["fin_wizard_step"] = 2
            st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun()

# ETAPA 2 – Produtos
elif step == 2:
    _step_card(
        "Etapa 2 — Cadastre Produtos/Insumos",
        "Registre compras/entradas dos insumos utilizados nos procedimentos (fornecedor, custo, quantidade).",
    )

    st.info("Abra a página de Produtos para cadastrar ou ver entradas:")
    _safe_page_link("pages/20_Produtos.py", "Abrir Produtos →", "📦")

    st.markdown("##### Situação dos dados")
    st.write("Concluído?: ", "✅" if has_step2 else "⏳")

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        if st.button("Voltar para Custos Fixos", use_container_width=True):
            st.session_state["fin_wizard_step"] = 1
            st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun()
    with c2:
        if st.button(
            "Marcar etapa como concluída", type="primary", use_container_width=True
        ):
            if not _has_products():
                st.warning("Não detectei entradas/produtos. Se já cadastrou, prossiga.")
            st.session_state["fin_wizard_step"] = 3
            _toast_and_success("Etapa 2 concluída. Vá para Procedimentos.", "✅")
    with c3:
        if (
            has_step1
            and has_step2
            and st.button("Pular (já concluído)", use_container_width=True)
        ):
            st.session_state["fin_wizard_step"] = 3
            st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun()

# ETAPA 3 – Procedimentos
elif step == 3:
    _step_card(
        "Etapa 3 — Cadastre Procedimentos",
        "Monte o catálogo, defina BOM (insumos), comissão, overhead e utilize o preview de margem.",
    )

    st.info("Abra a página de Procedimentos para cadastrar e lançar vendas:")
    # Procedimentos pode estar em página multipage ou módulo
    _safe_page_link("pages/10_Procedimentos.py", "Abrir Procedimentos →", "🧪")

    st.markdown("##### Situação dos dados")
    st.write("Concluído?: ", "✅" if has_step3 else "⏳")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Voltar para Produtos", use_container_width=True):
            st.session_state["fin_wizard_step"] = 2
            st.experimental_rerun() if hasattr(st, "experimental_rerun") else st.rerun()
    with c2:
        if st.button("Finalizar Workflow", type="primary", use_container_width=True):
            _toast_and_success("Workflow Financeiro concluído! 🎉", "🎉")

st.divider()

# Resumo
with st.expander("Resumo do progresso", expanded=False):
    st.write(
        {
            "custos_fixos": "ok" if has_step1 else "pendente",
            "produtos": "ok" if has_step2 else "pendente",
            "procedimentos": "ok" if has_step3 else "pendente",
            "passo_atual": st.session_state.get("fin_wizard_step"),
        }
    )
