import streamlit as st
import pandas as pd
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
    if buscar:
        st.session_state["df_fixos"] = read_fixed_costs(
            period=period if period else None,
            category=None if category == "(todas)" else category,
        )

    df = st.session_state.get("df_fixos", pd.DataFrame())

    if df.empty and buscar:
        st.info("Nenhum lançamento encontrado para os filtros atuais.")
    elif not df.empty:
        st.caption(f"{len(df)} registros encontrados")
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

        selected_ids = edited.loc[edited["Selecionar"], "fixed_cost_id"].tolist()

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            if st.button(
                "🗑️ Excluir selecionados",
                disabled=len(selected_ids) == 0,
                use_container_width=True,
            ):
                n = delete_fixed_cost(selected_ids)

                # ✅ Atualizar grid na hora
                if "df_fixos" in st.session_state:
                    df_local = st.session_state["df_fixos"]
                    # Remove os IDs excluídos do DataFrame atual
                    st.session_state["df_fixos"] = df_local[
                        ~df_local["fixed_cost_id"].isin(selected_ids)
                    ].reset_index(drop=True)

                st.success(f"🗑️ {n} registro(s) excluído(s) com sucesso.")
                st.rerun()  # força recarregar a página e redesenhar o grid atualizado

        with c2:
            st.button(
                "✏️ Alterar selecionados (em massa)",
                disabled=True,
                use_container_width=True,
            )

        with c3:
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
    def _options_with_suggestions(
        suggestions: list[str], existing: list[str]
    ) -> list[str]:
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
        [
            "Aluguel",
            "Energia",
            "Água",
            "Internet",
            "Telefone",
            "Softwares",
            "Manutenção",
        ],
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

    with st.form("form_novo_custo_fixo"):
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

        def _value_or_new(label: str, selected: str):
            if selected == "(+ novo)":
                return st.text_input(f"Novo {label}", key=f"novo_{label}").strip()
            return selected

        f_category = _value_or_new("Categoria", f_category_sel)
        payment_method = _value_or_new("Forma de pagamento", payment_method_sel)
        vendor = _value_or_new("Fornecedor", vendor_sel)
        cost_center = _value_or_new("Centro de custo", cost_center_sel)

        # Combo de recorrência
        recurrence_options = [
            "Único",
            "Semanal",
            "Quinzenal",
            "Mensal",
            "Bimestral",
            "Trimestral",
            "Semestral",
            "Anual",
        ]
        recurrence = st.selectbox(
            "Recorrência",
            recurrence_options,
            index=3,  # deixa "Mensal" como padrão
            help="Selecione a frequência com que o custo se repete.",
        )

        due_day = st.number_input(
            "Dia de vencimento", min_value=1, max_value=31, step=1
        )
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

    st.divider()

    # -------------------- TEMPLATE (fora de form) --------------------
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

    template_csv = sample.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Baixar template (CSV)",
        data=template_csv,
        file_name="template_custos_fixos.csv",
        mime="text/csv",
        help="Baixe o layout padrão para carga de custos fixos.",
    )

    st.markdown("### 🚚 Carga de arquivo")
    up = st.file_uploader(
        "Envie CSV/XLSX no layout padrão", type=["csv", "xls", "xlsx"]
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

            required = {"period", "date", "description", "category", "amount"}
            norm_cols = {c.strip().lower() for c in raw.columns}
            missing = required - norm_cols

            if missing:
                st.error(f"Colunas obrigatórias ausentes: {', '.join(sorted(missing))}")
            else:
                if st.button("Carregar arquivo", type="primary"):
                    res = upsert_fixed_costs(raw)
                    st.success(
                        f"Carga concluída. Inseridos: {res['inserted']} | Atualizados: {res['updated']} | Erros: {res['errors']}"
                    )
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")

    st.divider()
    st.markdown("### ⚙️ Gerenciar Combos (Dimensões)")

    dim_map = {
        "Categoria": ("category", list_categories),
        "Fornecedor": ("vendor", list_vendors),
        "Centro de Custo": ("cost_center", list_cost_centers),
        "Forma de Pagamento": ("payment_method", list_payment_methods),
    }

    col1, col2, col3 = st.columns([1, 1, 0.8])
    dim_label = col1.selectbox("Selecione a dimensão", list(dim_map.keys()))
    dim_name, list_func = dim_map[dim_label]

    items = list_func()
    selected_item = col2.selectbox("Item", items if items else ["(nenhum disponível)"])

    action = col3.radio(
        "Ação", ["Desativa", "Reativar"], horizontal=True, key=f"radio_{dim_name}"
    )

    if st.button("🗑️ Executar ação", type="primary", use_container_width=True):
        try:
            from src.dataio.fixed_costs import pd, DIM_DIR, utc_now

            path = DIM_DIR / f"dim_{dim_name}.parquet"
            df = pd.read_parquet(path)
            code_col = f"{dim_name}_code"
            is_active_col = "is_active"

            # Encontrar registro (case-insensitive)
            mask = (
                df[code_col].astype(str).str.lower()
                == str(selected_item).lower().strip()
            )
            if not mask.any():
                st.warning(f"Item '{selected_item}' não encontrado.")
            else:
                df.loc[mask, is_active_col] = True if action == "Reativar" else False
                df.loc[mask, "updated_at"] = utc_now()
                df.to_parquet(path, index=False)
                st.success(f"Item '{selected_item}' {action.lower()}do com sucesso!")
        except Exception as e:
            st.error(f"Erro ao processar: {e}")
