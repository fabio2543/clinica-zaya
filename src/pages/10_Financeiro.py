import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
from src.dataio.fixed_costs import (
    read_fixed_costs,
    upsert_fixed_costs,
    create_fixed_cost,
    delete_fixed_cost,
    list_categories,
    list_cost_centers,
    list_payment_methods,
    list_vendors,
)

st.set_page_config(page_title="Módulo Financeiro", layout="wide")
st.title("💰 Módulo Financeiro")
st.caption("Central financeiro: Custos Fixos, Produtos e Procedimentos.")

tab_fixos, tab_produtos, tab_proced = st.tabs(
    ["🧾 Custos Fixos", "🧪 Produtos", "🧮 Procedimentos"]
)

with tab_fixos:
    st.subheader("🧾 Custos Fixos")

    # -------------------- FILTROS --------------------
    cols = st.columns([1, 1, 1, 0.5])
    period = cols[0].text_input("Período (AAAA-MM)", value="")
    cat_options = ["(todas)"] + list_categories()
    category = cols[1].selectbox("Categoria", cat_options, index=0)
    show_deleted = cols[2].checkbox("Mostrar excluídos", value=False, disabled=True)
    buscar = cols[3].button("🔍 Buscar", use_container_width=True)

    st.divider()

    # -------------------- LISTAGEM --------------------
    df = pd.DataFrame()
    if buscar:  # Só executa a busca quando clicar no botão
        df = read_fixed_costs(
            period=period if period else None,
            category=None if category == "(todas)" else category,
        )
    if df.empty:
        st.info("Nenhum lançamento encontrado para os filtros atuais.")
    else:
        st.caption(f"{len(df)} registros")
        # seleção múltipla
        df_view = df.copy()
        df_view["Selecionar"] = False
        edited = st.data_editor(
            df_view,
            column_config={
                "Selecionar": st.column_config.CheckboxColumn(required=False)
            },
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            key="grid_fixos",
        )
        selected_ids = edited.loc[
            edited["Selecionar"] == True, "fixed_cost_id"
        ].tolist()

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button(
                "🗑️ Excluir selecionados",
                disabled=len(selected_ids) == 0,
                use_container_width=True,
            ):
                n = delete_fixed_cost(selected_ids)
                st.success(
                    f"Excluídos {n} registro(s). Atualize os filtros para recarregar."
                )
        with c2:
            st.button(
                "✏️ Alterar selecionados (em massa)",
                disabled=True,
                use_container_width=True,
                help="Próximo passo: tela de edição em massa",
            )
        with c3:
            # Exportar resultado do filtro
            if st.button("⬇️ Exportar CSV", use_container_width=True):
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Baixar CSV filtrado",
                    data=csv_bytes,
                    file_name=f"custos_fixos_{datetime.now():%Y%m%d}.csv",
                    mime="text/csv",
                )
        st.divider()

# -------------------- CADASTRO UNITÁRIO --------------------
st.markdown("### ➕ Cadastro unitário")


# utilitário para unir sugestões + existentes sem duplicar
def _options_with_suggestions(suggestions: list[str], existing: list[str]) -> list[str]:
    opts = ["(+ novo)"]
    seen = set()
    for x in suggestions + existing:
        if not x or x in seen:
            continue
        seen.add(x)
        opts.append(x)
    return opts


# montar opções (sugestões + existentes)
cat_opts = _options_with_suggestions(
    ["Aluguel", "Energia", "Água", "Internet", "Telefone", "Softwares", "Manutenção"],
    list_categories(),
)
pm_opts = _options_with_suggestions(
    ["Pix", "Boleto", "Cartão", "TED", "Dinheiro"], list_payment_methods()
)
ven_opts = _options_with_suggestions(
    ["Imobiliária XYZ", "Concessionária de Energia", "Operadora de Internet"],
    list_vendors(),
)
cc_opts = _options_with_suggestions(
    ["Administrativo", "Recepção", "Sala 1", "Sala 2", "Comercial", "Financeiro"],
    list_cost_centers(),
)

with st.form("novo_custo_fixo"):
    c1, c2, c3 = st.columns(3)
    f_period = c1.text_input("Competência (AAAA-MM)", placeholder="2025-10")
    f_date = c2.date_input("Data")
    f_category_sel = c3.selectbox(
        "Categoria", cat_opts, index=1 if len(cat_opts) > 1 else 0
    )

    description = st.text_input("Descrição", placeholder="Ex.: Aluguel da sala")
    amount = st.number_input("Valor (R$)", min_value=0.0, step=50.0, format="%.2f")

    c4, c5, c6 = st.columns(3)
    payment_method_sel = c4.selectbox(
        "Forma de pagamento", pm_opts, index=1 if len(pm_opts) > 1 else 0
    )
    vendor_sel = c5.selectbox(
        "Fornecedor", ven_opts, index=1 if len(ven_opts) > 1 else 0
    )
    cost_center_sel = c6.selectbox(
        "Centro de custo", cc_opts, index=1 if len(cc_opts) > 1 else 0
    )

    # se usuário escolher "(+ novo)", abre campo para digitar o novo valor
    def _value_or_new(label: str, selected: str):
        if selected == "(+ novo)":
            return st.text_input(f"Novo {label}", key=f"novo_{label}").strip()
        return selected

    f_category = _value_or_new("Categoria", f_category_sel)
    payment_method = _value_or_new("Forma de pagamento", payment_method_sel)
    vendor = _value_or_new("Fornecedor", vendor_sel)
    cost_center = _value_or_new("Centro de custo", cost_center_sel)

    recurrence = st.text_input("Recorrência", placeholder="mensal/anual/único")
    due_day = st.number_input("Dia de vencimento", min_value=1, max_value=31, step=1)
    notes = st.text_input("Observações", placeholder="")

    submit = st.form_submit_button("Salvar")

    if submit:
        if not f_period or not description or amount <= 0:
            st.error("Preencha pelo menos: **Competência, Descrição e Valor**.")
        else:
            row = {
                "period": f_period,
                "date": f_date.isoformat(),
                "description": description,
                "category": f_category,
                "amount": amount,
                "payment_method": payment_method,
                "vendor": vendor,
                "recurrence": recurrence,
                "due_day": int(due_day),
                "cost_center": cost_center,
                "invoice_number": None,
                "notes": notes,
            }
            res = create_fixed_cost(row)
            st.success(
                f"Registro gravado. Inseridos: {res.get('inserted', 0)}, "
                f"Atualizados: {res.get('updated', 0)}"
            )
    # -------------------- IMPORTAÇÃO MASSIVA --------------------
    st.markdown("### 📥 Importação (CSV/XLSX) + Template")
    sample = pd.DataFrame(
        [
            {
                "period": datetime.now().strftime("%Y-%m"),
                "date": datetime.now().date().isoformat(),
                "description": "Aluguel da sala",
                "category": "Aluguel",
                "amount": 4300.00,
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
    )

    up = st.file_uploader(
        "Envie CSV/XLS/XLSX no layout padrão", type=["csv", "xls", "xlsx"]
    )
    if up:
        try:
            raw = (
                pd.read_csv(up)
                if up.name.lower().endswith(".csv")
                else pd.read_excel(up)
            )
            st.write("Prévia do arquivo:")
            st.dataframe(raw.head(50), use_container_width=True)

            # validação mínima
            required = {"period", "date", "description", "category", "amount"}
            missing = required - set(map(str.lower, raw.columns))
            if missing:
                st.error(f"Colunas obrigatórias ausentes: {', '.join(missing)}")
            else:
                if st.button("Carregar arquivo", type="primary"):
                    res = upsert_fixed_costs(raw)
                    st.success(
                        f"Carga concluída. Inseridos: {res['inserted']} | Atualizados: {res['updated']} | Erros: {res['errors']}"
                    )
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")
