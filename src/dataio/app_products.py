
import os, json, uuid, io
from datetime import datetime, date
import pandas as pd
import streamlit as st

BASE_DIR = "/mnt/data/zaya_products"
PARQUET_PATH = os.path.join(BASE_DIR, "products.parquet")
SCHEMA_COLUMNS = [
    "id","product_name","unit_price","purchase_date","quantity",
    "sku","category","notes","status","is_deleted","created_at","updated_at"
]

def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _empty_df():
    return pd.DataFrame(columns=SCHEMA_COLUMNS)

def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if "unit_price" in df.columns:
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0).astype(float)
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
    if "purchase_date" in df.columns:
        def norm_date(x):
            if pd.isna(x) or x == "":
                return ""
            try:
                if isinstance(x, (pd.Timestamp, datetime, date)):
                    return pd.to_datetime(x).date().isoformat()
                return pd.to_datetime(str(x)).date().isoformat()
            except Exception:
                return ""
        df["purchase_date"] = df["purchase_date"].apply(norm_date)
    for col in ["sku","category","notes"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    if "status" in df.columns:
        df["status"] = df["status"].fillna("active").astype(str)
    return df

def _validate_rows(df: pd.DataFrame) -> pd.DataFrame:
    required = ["product_name","unit_price","purchase_date","quantity"]
    out = df.copy()
    errors = []
    for _, row in out.iterrows():
        row_errs = []
        for col in required:
            if pd.isna(row.get(col, None)) or str(row.get(col)).strip() == "" or (col in ["unit_price","quantity"] and float(row.get(col, 0)) < 0):
                row_errs.append(f"Campo obrigatório inválido: {col}")
        try:
            pd.to_datetime(row.get("purchase_date"))
        except Exception:
            row_errs.append("purchase_date inválida")
        st = str(row.get("status","active")).lower()
        if st not in ["active","inactive"]:
            row_errs.append("status deve ser active|inactive")
        errors.append("; ".join(row_errs))
    out["row_errors"] = errors
    return out

def load_products() -> pd.DataFrame:
    if not os.path.exists(PARQUET_PATH):
        return _empty_df()
    try:
        df = pd.read_parquet(PARQUET_PATH)
        for c in SCHEMA_COLUMNS:
            if c not in df.columns:
                df[c] = "" if c not in ["unit_price","quantity","is_deleted"] else (0.0 if c=="unit_price" else (0 if c=="quantity" else False))
        df = df[SCHEMA_COLUMNS]
        return df
    except Exception:
        return _empty_df()

def save_products(df: pd.DataFrame):
    df = df.copy()
    for c in SCHEMA_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[SCHEMA_COLUMNS]
    df.to_parquet(PARQUET_PATH, index=False)

st.set_page_config(page_title="Zaya | Produtos (Básico)", layout="wide")
st.title("Produtos — Cadastro Básico")
st.caption("Consulta • Inserção • Atualização • Exclusão • Carga (CSV/XLSX)")

# Sidebar navigation
menu = st.sidebar.radio("Menu", ["Consulta","Inserção","Atualização","Exclusão","Carga (CSV/XLSX)"])

df = load_products()

def grid_view(df_view: pd.DataFrame):
    st.dataframe(df_view, use_container_width=True, hide_index=True)

if menu == "Consulta":
    st.subheader("Consulta de Produtos")
    col1, col2, col3 = st.columns(3)
    with col1:
        q_name = st.text_input("Buscar por nome/sku (contém)", "")
    with col2:
        status_filter = st.selectbox("Status", ["Todos","active","inactive"])
    with col3:
        hide_deleted = st.checkbox("Ocultar excluídos", True)

    q = df.copy()
    if q_name.strip():
        s = q_name.strip().lower()
        q = q[q["product_name"].str.lower().str.contains(s) | q["sku"].str.lower().str.contains(s)]
    if status_filter != "Todos":
        q = q[q["status"].str.lower() == status_filter]
    if hide_deleted:
        q = q[q["is_deleted"] != True]

    # Basic KPIs
    total_items = int(q.shape[0])
    total_qty = int(q["quantity"].sum()) if not q.empty else 0
    total_value = float((q["unit_price"] * q["quantity"]).sum()) if not q.empty else 0.0
    k1, k2, k3 = st.columns(3)
    k1.metric("Registros", f"{total_items}")
    k2.metric("Quantidade total", f"{total_qty}")
    k3.metric("Valor total (R$)", f"{total_value:,.2f}".replace(",", "X").replace(".", ",").replace("X","."))

    grid_view(q.drop(columns=["is_deleted"]))

    # Export
    exp1, exp2 = st.columns(2)
    with exp1:
        st.download_button("Exportar CSV", q.to_csv(index=False).encode("utf-8"), "produtos_export.csv", "text/csv")
    with exp2:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
            q.to_excel(writer, index=False, sheet_name="Produtos")
        st.download_button("Exportar Excel", data=buf.getvalue(), file_name="produtos_export.xlsx")

elif menu == "Inserção":
    st.subheader("Inserir novo produto")
    with st.form("form_insert"):
        product_name = st.text_input("Nome do produto *")
        unit_price = st.number_input("Preço unitário (R$) *", min_value=0.0, step=0.01, format="%.2f")
        purchase_date = st.date_input("Data da compra *", value=date.today())
        quantity = st.number_input("Quantidade *", min_value=0, step=1)
        sku = st.text_input("SKU (opcional)")
        category = st.text_input("Categoria (opcional)")
        notes = st.text_area("Observações (opcional)", height=80)
        status = st.selectbox("Status", ["active","inactive"])
        submitted = st.form_submit_button("Salvar")

    if submitted:
        new_row = {
            "id": str(uuid.uuid4()),
            "product_name": product_name.strip(),
            "unit_price": float(unit_price),
            "purchase_date": purchase_date.isoformat(),
            "quantity": int(quantity),
            "sku": sku.strip(),
            "category": category.strip(),
            "notes": notes.strip(),
            "status": status,
            "is_deleted": False,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        tmp = pd.DataFrame([new_row])
        tmp = _coerce_types(tmp)
        val = _validate_rows(tmp)
        if val["row_errors"].iloc[0] != "":
            st.error(f"Erro de validação: {val['row_errors'].iloc[0]}")
        else:
            df2 = pd.concat([df, tmp], ignore_index=True)
            save_products(df2)
            st.success("Produto inserido com sucesso.")

elif menu == "Atualização":
    st.subheader("Atualizar produto existente")
    base = df[df["is_deleted"] != True].copy()
    if base.empty:
        st.info("Não há produtos para atualizar.")
    else:
        choices = base.apply(lambda r: f"{r['product_name']} | {r['sku']} | {r['purchase_date']} | id={r['id']}", axis=1).tolist()
        selected = st.selectbox("Selecione um registro", choices)
        sel_id = selected.split("id=")[-1]
        rec = base[base["id"] == sel_id].iloc[0]

        with st.form("form_update"):
            product_name = st.text_input("Nome do produto *", rec["product_name"])
            unit_price = st.number_input("Preço unitário (R$) *", min_value=0.0, step=0.01, value=float(rec["unit_price"]), format="%.2f")
            purchase_date = st.date_input("Data da compra *", value=pd.to_datetime(rec["purchase_date"]).date())
            quantity = st.number_input("Quantidade *", min_value=0, step=1, value=int(rec["quantity"]))
            sku = st.text_input("SKU (opcional)", rec["sku"])
            category = st.text_input("Categoria (opcional)", rec["category"])
            notes = st.text_area("Observações (opcional)", rec["notes"], height=80)
            status = st.selectbox("Status", ["active","inactive"], index=0 if rec["status"]=="active" else 1)
            submitted = st.form_submit_button("Atualizar")

        if submitted:
            idx = df.index[df["id"] == sel_id][0]
            df.loc[idx, ["product_name","unit_price","purchase_date","quantity","sku","category","notes","status","updated_at"]] = [
                product_name.strip(),
                float(unit_price),
                purchase_date.isoformat(),
                int(quantity),
                sku.strip(),
                category.strip(),
                notes.strip(),
                status,
                _now_iso(),
            ]
            # validação
            val = _validate_rows(pd.DataFrame([df.loc[idx]]))
            if val["row_errors"].iloc[0] != "":
                st.error(f"Erro de validação: {val['row_errors'].iloc[0]}")
            else:
                save_products(df)
                st.success("Registro atualizado.")

elif menu == "Exclusão":
    st.subheader("Exclusão (soft delete)")
    base = df[df["is_deleted"] != True].copy()
    if base.empty:
        st.info("Não há produtos para excluir.")
    else:
        choices = base.apply(lambda r: f"{r['product_name']} | {r['sku']} | {r['purchase_date']} | id={r['id']}", axis=1).tolist()
        selected = st.selectbox("Selecione um registro", choices)
        sel_id = selected.split("id=")[-1]
        if st.button("Marcar como excluído"):
            idx = df.index[df["id"] == sel_id][0]
            df.loc[idx, "is_deleted"] = True
            df.loc[idx, "updated_at"] = _now_iso()
            save_products(df)
            st.success("Registro marcado como excluído.")

elif menu == "Carga (CSV/XLSX)":
    st.subheader("Carga de Arquivo")
    st.caption("Aceita CSV ou Excel com as colunas: product_name, unit_price, purchase_date, quantity, sku, category, notes, status")
    up = st.file_uploader("Selecione o arquivo", type=["csv","xlsx"])
    if up is not None:
        try:
            if up.name.lower().endswith(".csv"):
                raw = pd.read_csv(up)
            else:
                raw = pd.read_excel(up)
            raw = raw.rename(columns={c: c.strip() for c in raw.columns})
            expected = ["product_name","unit_price","purchase_date","quantity","sku","category","notes","status"]
            for c in expected:
                if c not in raw.columns:
                    raw[c] = "" if c not in ["unit_price","quantity"] else 0
            raw = raw[expected]
            raw = _coerce_types(raw)
            val = _validate_rows(raw)
            errs = val[val["row_errors"]!=""]
            st.write("Pré-visualização:")
            st.dataframe(val, use_container_width=True, hide_index=True)
            if not errs.empty:
                st.error(f"Foram encontrados {errs.shape[0]} erros. Corrija e reenvie o arquivo.")
            else:
                # merge strategy: insert all rows as new records (idempotence can be added later via key)
                now = _now_iso()
                to_add = raw.copy()
                to_add.insert(0, "id", [str(uuid.uuid4()) for _ in range(to_add.shape[0])])
                to_add["is_deleted"] = False
                to_add["created_at"] = now
                to_add["updated_at"] = now

                final = pd.concat([load_products(), to_add], ignore_index=True)
                # Ensure schema and save
                for c in SCHEMA_COLUMNS:
                    if c not in final.columns:
                        final[c] = None
                final = final[SCHEMA_COLUMNS]
                final.to_parquet(PARQUET_PATH, index=False)
                st.success(f"Carga realizada com sucesso. Linhas inseridas: {to_add.shape[0]}")
        except Exception as e:
            st.error(f"Falha ao processar arquivo: {e}")
