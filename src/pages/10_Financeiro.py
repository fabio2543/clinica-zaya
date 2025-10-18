import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

st.set_page_config(page_title="Módulo Financeiro", layout="wide")
st.title("💰 Módulo Financeiro")
st.caption("Central financeiro: Custos Fixos, Produtos e Procedimentos.")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

tab_fixos, tab_produtos, tab_proced = st.tabs(
    ["🧾 Custos Fixos", "🧪 Produtos", "🧮 Procedimentos"]
)

# =========================
# 🧾 CUSTOS FIXOS
# =========================
with tab_fixos:
    st.subheader("Custos Fixos")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("**Importar arquivo (CSV/XLS/XLSX)**")
        up = st.file_uploader(
            "Selecione um arquivo", type=["csv", "xls", "xlsx"], key="fixos_up"
        )
        if up:
            try:
                df = (
                    pd.read_csv(up)
                    if up.name.lower().endswith(".csv")
                    else pd.read_excel(up)
                )
                st.success(f"Arquivo lido com {len(df)} linhas.")
                st.dataframe(df.head(100), use_container_width=True)
            except Exception as e:
                st.error(f"Erro ao ler arquivo: {e}")

    with c2:
        st.markdown("**Layout padrão**")
        st.code(
            "period, date, description, category, amount, subcategory, payment_method, vendor, recurrence, due_day, cost_center, invoice_number, notes"
        )

        # botão p/ baixar template (mínimo)
        sample = pd.DataFrame(
            [
                {
                    "period": datetime.now().strftime("%Y-%m"),
                    "date": datetime.now().date().isoformat(),
                    "description": "Aluguel da sala",
                    "category": "Aluguel",
                    "amount": 4300.00,
                    "subcategory": "",
                    "payment_method": "Boleto",
                    "vendor": "Imobiliária XYZ",
                    "recurrence": "mensal",
                    "due_day": 5,
                    "cost_center": "Administrativo",
                    "invoice_number": "",
                    "notes": "Contrato 12 meses",
                }
            ]
        )
        st.download_button(
            "⬇️ Baixar template (CSV)",
            data=sample.to_csv(index=False).encode("utf-8"),
            file_name="template_custos_fixos.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown("---")
    st.markdown("**Inclusão manual (mock)**")
    with st.form("novo_custo_fixo"):
        c1, c2, c3 = st.columns(3)
        period = c1.text_input("Competência (AAAA-MM)", placeholder="2025-10")
        date = c2.date_input("Data")
        category = c3.selectbox(
            "Categoria",
            [
                "Aluguel",
                "Energia",
                "Internet",
                "Pessoal",
                "Contabilidade",
                "Limpeza",
                "Marketing",
                "Softwares",
                "Segurança",
                "Outros",
            ],
        )
        description = st.text_input("Descrição", placeholder="Ex.: Aluguel da sala")
        amount = st.number_input("Valor (R$)", min_value=0.0, step=50.0, format="%.2f")
        sub = st.form_submit_button("Adicionar")
        if sub:
            st.success(
                "Lançamento registrado (mock). Próxima etapa: persistir em data/custos_fixos.parquet e listar abaixo."
            )

# =========================
# 🧪 PRODUTOS
# =========================
with tab_produtos:
    st.subheader("Produtos (insumos)")

    st.markdown("**Importar base de produtos**")
    up_prod = st.file_uploader(
        "CSV/XLS/XLSX", type=["csv", "xls", "xlsx"], key="prod_up"
    )
    if up_prod:
        try:
            dfp = (
                pd.read_csv(up_prod)
                if up_prod.name.endswith(".csv")
                else pd.read_excel(up_prod)
            )
            st.success(f"Arquivo lido com {len(dfp)} itens.")
            st.dataframe(dfp.head(100), use_container_width=True)
        except Exception as e:
            st.error(f"Erro: {e}")

    st.markdown("**Campos sugeridos**")
    st.code(
        "sku, nome, categoria, marca, unidade (un/ml/g), custo_unitario, validade_opcional"
    )

    st.markdown("---")
    st.markdown("**Cadastro manual (mock)**")
    with st.form("novo_produto"):
        c1, c2, c3 = st.columns(3)
        sku = c1.text_input("SKU/Código")
        nome = c2.text_input("Nome do produto")
        categoria = c3.text_input(
            "Categoria", placeholder="Bioestimulador, Toxina, Skincare…"
        )
        c4, c5, c6 = st.columns(3)
        marca = c4.text_input("Marca")
        unidade = c5.selectbox("Unidade", ["un", "ml", "g"])
        custo = c6.number_input(
            "Custo unitário (R$)", min_value=0.0, step=10.0, format="%.2f"
        )
        ok = st.form_submit_button("Salvar")
        if ok:
            st.success(
                "Produto salvo (mock). Próxima etapa: gravar em data/produtos.parquet e listar abaixo."
            )

# =========================
# 🧮 PROCEDIMENTOS
# =========================
with tab_proced:
    st.subheader("Procedimentos")

    st.markdown("**Estrutura sugerida (ficha técnica)**")
    st.code(
        "procedimento, duracao_min, profissional, custo_hora_profissional, "
        "item_1_sku, item_1_qtd, item_2_sku, item_2_qtd, ..., custos_fixos_rateio_opcional"
    )

    st.markdown("---")
    st.markdown("**Montar procedimento (mock)**")
    with st.form("novo_proc"):
        c1, c2, c3 = st.columns(3)
        nome_proc = c1.text_input("Nome do procedimento")
        duracao = c2.number_input("Duração (min)", min_value=0, step=5)
        custo_hora = c3.number_input(
            "Custo/hora do profissional (R$)", min_value=0.0, step=10.0, format="%.2f"
        )
        calc = st.form_submit_button("Calcular custo (mock)")
        if calc:
            mao_obra = (duracao / 60.0) * custo_hora
            st.info(f"Custo de mão de obra estimado: **R$ {mao_obra:,.2f}**")

    st.caption(
        "Próxima etapa: selecionar itens do catálogo de Produtos para compor consumo e custo total por procedimento."
    )
