import os
import io
import uuid
import pandas as pd
import streamlit as st
from datetime import date

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="Controle Financeiro", layout="centered")

ARQUIVO = "dados.xlsx"

PAGAMENTOS = ["PIX", "Cartão Pão de Açucar", "Cartão Nubank", "Swile", "Pluxee"]
PESSOAS = ["Roney", "Adriele"]

MESES = [
    (1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"),
    (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"),
    (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro"),
]
MES_NOME = {n: nome for n, nome in MESES}

GASTOS_COLS = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]
METAS_COLS = ["Categoria", "Meta"]

# -----------------------------
# FUNÇÕES
# -----------------------------
def _normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _ensure_gastos_schema(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    changed = False
    df = _normalizar_colunas(df)

    for c in GASTOS_COLS:
        if c not in df.columns:
            df[c] = pd.NA
            changed = True

    # ID
    df["ID"] = df["ID"].astype("string")
    faltando = df["ID"].isna() | (df["ID"].str.strip() == "")
    if faltando.any():
        df.loc[faltando, "ID"] = [uuid.uuid4().hex for _ in range(int(faltando.sum()))]
        changed = True

    # Tipos
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

    for c in ["Categoria", "Subcategoria", "Pagamento", "Quem", "Obs"]:
        df[c] = df[c].astype("string").fillna("").astype(str)

    return df[GASTOS_COLS].copy(), changed

def _ensure_metas_schema(metas: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    changed = False
    metas = _normalizar_colunas(metas)

    for c in METAS_COLS:
        if c not in metas.columns:
            metas[c] = pd.NA
            changed = True

    metas["Categoria"] = metas["Categoria"].astype("string").fillna("").astype(str).str.strip()
    metas["Meta"] = pd.to_numeric(metas["Meta"], errors="coerce").fillna(0.0)

    return metas[METAS_COLS].copy(), changed

def salvar_excel(df: pd.DataFrame, metas: pd.DataFrame, arquivo: str = ARQUIVO):
    df2, _ = _ensure_gastos_schema(df)
    metas2, _ = _ensure_metas_schema(metas)

    with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
        df2.to_excel(writer, sheet_name="gastos", index=False)
        metas2.to_excel(writer, sheet_name="metas", index=False)

def init_arquivo_se_faltar():
    if os.path.exists(ARQUIVO):
        return
    df_vazio = pd.DataFrame(columns=GASTOS_COLS)
    metas_padrao = pd.DataFrame(
        {"Categoria": ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros", "Geral"],
         "Meta": [0, 0, 0, 0, 0, 0]}
    )
    salvar_excel(df_vazio, metas_padrao, ARQUIVO)

@st.cache_data(show_spinner=False)
def carregar_excel_cached(path: str, mtime: float):
    df = pd.read_excel(path, sheet_name="gastos")
    metas = pd.read_excel(path, sheet_name="metas")
    return df, metas

def carregar_excel():
    init_arquivo_se_faltar()
    mtime = os.path.getmtime(ARQUIVO)
    df_raw, metas_raw = carregar_excel_cached(ARQUIVO, mtime)

    df, df_changed = _ensure_gastos_schema(df_raw)
    metas, metas_changed = _ensure_metas_schema(metas_raw)

    # Se precisou corrigir schema/IDs, grava uma vez e recarrega (evita inconsistência)
    if df_changed or metas_changed:
        salvar_excel(df, metas, ARQUIVO)
        st.cache_data.clear()
        st.rerun()

    return df, metas

def excel_bytes(df: pd.DataFrame, metas: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="gastos", index=False)
        metas.to_excel(writer, sheet_name="metas", index=False)
    return buffer.getvalue()

def filtro_periodo(df: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    dfx = df.copy()
    dfx["Data_dt"] = pd.to_datetime(dfx["Data"], errors="coerce")
    mask = (dfx["Data_dt"].dt.year == ano) & (dfx["Data_dt"].dt.month == mes)
    out = dfx.loc[mask].copy()
    out.drop(columns=["Data_dt"], inplace=True, errors="ignore")
    out, _ = _ensure_gastos_schema(out)
    return out

def restore_from_upload(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_up = pd.read_excel(uploaded_file, sheet_name="gastos")
    metas_up = pd.read_excel(uploaded_file, sheet_name="metas")
    df_up, _ = _ensure_gastos_schema(df_up)
    metas_up, _ = _ensure_metas_schema(metas_up)
    salvar_excel(df_up, metas_up, ARQUIVO)
    return df_up, metas_up

# -----------------------------
# APP
# -----------------------------
df, metas = carregar_excel()

# categorias
cats = metas["Categoria"].dropna().astype(str).str.strip().tolist()
cats_lanc = [c for c in cats if c.lower() != "geral" and c != ""]
if not cats_lanc:
    cats_lanc = ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros"]

# filtros de ano/mês (sem fazer coisa pesada)
hoje = date.today()
df_tmp = df.copy()
df_tmp["Data_dt"] = pd.to_datetime(df_tmp["Data"], errors="coerce")
anos = sorted([int(a) for a in df_tmp["Data_dt"].dt.year.dropna().unique().tolist()])
if hoje.year not in anos:
    anos = sorted(list(set(anos + [hoje.year])))

with st.sidebar:
    menu = st.radio("Menu", ["Lançar", "Resumo", "Gerenciar", "Backup/Restore"], index=0)

    st.divider()
    st.subheader("Período")
    ano_sel = st.selectbox("Ano", anos, index=anos.index(hoje.year))
    mes_sel = st.selectbox("Mês", [m for m, _ in MESES], index=hoje.month - 1, format_func=lambda m: MES_NOME[m])

st.title("Controle Financeiro do Casal")

# -----------------------------
# Lançar
# -----------------------------
if menu == "Lançar":
    st.subheader("Novo gasto")

    with st.form("novo_gasto"):
        data_lcto = st.date_input("Data", date.today())
        categoria = st.selectbox("Categoria", cats_lanc)
        sub = st.text_input("Subcategoria (opcional)")
        valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
        quem = st.selectbox("Quem pagou", PESSOAS)
        pagamento = st.selectbox("Forma de pagamento", PAGAMENTOS)
        obs = st.text_input("Observação")
        salvar = st.form_submit_button("Salvar gasto")

    if salvar:
        novo = {
            "ID": uuid.uuid4().hex,
            "Data": data_lcto,
            "Categoria": categoria,
            "Subcategoria": sub,
            "Valor": float(valor),
            "Pagamento": pagamento,
            "Quem": quem,
            "Obs": obs,
        }
        df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
        salvar_excel(df, metas, ARQUIVO)
        st.cache_data.clear()
        st.success("Gasto salvo.")
        st.rerun()

    st.divider()
    st.subheader("Últimos lançamentos (geral)")
    df_show = df.copy()
    df_show["Data_dt"] = pd.to_datetime(df_show["Data"], errors="coerce")
    st.dataframe(
        df_show.sort_values("Data_dt", ascending=False)[["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]].head(30),
        use_container_width=True
    )

# -----------------------------
# Resumo
# -----------------------------
elif menu == "Resumo":
    st.subheader(f"Resumo: {MES_NOME[mes_sel]}/{ano_sel}")

    df_periodo = filtro_periodo(df, ano_sel, mes_sel)
    total = float(df_periodo["Valor"].sum()) if not df_periodo.empty else 0.0
    st.metric("Total no período", f"R$ {total:,.2f}")

    if df_periodo.empty:
        st.info("Sem lançamentos neste período.")
    else:
        st.divider()
        st.subheader("Por categoria")
        resumo_cat = (
            df_periodo.groupby("Categoria")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        st.dataframe(resumo_cat, use_container_width=True)

        st.subheader("Por pessoa")
        resumo_pessoa = (
            df_periodo.groupby("Quem")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        st.dataframe(resumo_pessoa, use_container_width=True)

        st.subheader("Por forma de pagamento")
        resumo_pag = (
            df_periodo.groupby("Pagamento")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        st.dataframe(resumo_pag, use_container_width=True)

# -----------------------------
# Gerenciar (editar/apagar)
# -----------------------------
elif menu == "Gerenciar":
    st.subheader("Gerenciar lançamentos (editar / apagar)")

    df_periodo = filtro_periodo(df, ano_sel, mes_sel)

    col1, col2 = st.columns([1, 1])
    with col1:
        editar_todos = st.checkbox("Editar todos (não só o período)", value=False)
    with col2:
        mostrar_id = st.checkbox("Mostrar ID", value=False)

    df_view = df.copy() if editar_todos else df_periodo.copy()

    if df_view.empty:
        st.info("Sem lançamentos para este filtro.")
    else:
        st.caption("Para apagar: remova a linha na tabela e clique em 'Salvar alterações'. Para apagar com segurança, use 'Mostrar ID' ligado.")

        cols_show = GASTOS_COLS if mostrar_id else [c for c in GASTOS_COLS if c != "ID"]

        df_editor_base = df_view.copy()
        df_editor_base["Data"] = pd.to_datetime(df_editor_base["Data"], errors="coerce").dt.strftime("%Y-%m-%d")

        edited = st.data_editor(
            df_editor_base[cols_show],
            num_rows="dynamic",
            use_container_width=True,
            key=f"editor_{'ALL' if editar_todos else 'PER'}_{ano_sel}_{mes_sel}",
        )

        # recoloca ID se oculto (para conseguir salvar no mesmo padrão)
        if not mostrar_id:
            edited = edited.copy()
            edited.insert(0, "ID", df_editor_base["ID"].iloc[: len(edited)].values)

        b1, b2 = st.columns([1, 2])
        with b1:
            salvar_alt = st.button("Salvar alterações", type="primary")
        with b2:
            st.caption("Se duas pessoas editarem ao mesmo tempo, o último a salvar prevalece.")

        if salvar_alt:
            try:
                for c in GASTOS_COLS:
                    if c not in edited.columns:
                        edited[c] = ""

                edited = edited[GASTOS_COLS].copy()
                edited["Data"] = pd.to_datetime(edited["Data"], errors="coerce").dt.date
                edited["Valor"] = pd.to_numeric(edited["Valor"], errors="coerce").fillna(0.0)

                base = df.copy()
                base, _ = _ensure_gastos_schema(base)

                edited_ids = set(edited["ID"].astype(str).tolist())
                base_keep = base.loc[~base["ID"].astype(str).isin(edited_ids)].copy()

                final_df = pd.concat([base_keep, edited], ignore_index=True)
                final_df, _ = _ensure_gastos_schema(final_df)

                salvar_excel(final_df, metas, ARQUIVO)
                st.cache_data.clear()
                st.success("Alterações salvas.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar alterações: {e}")

        st.divider()
        st.subheader("Excluir lançamento rápido")

        df_sel = df_view.copy()
        df_sel["DataStr"] = pd.to_datetime(df_sel["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
        df_sel["Rotulo"] = df_sel.apply(
            lambda r: f"{r['DataStr']} | {r['Categoria']} | R$ {float(r['Valor']):,.2f} | {r['Quem']} | {r['Pagamento']} | ID={r['ID']}",
            axis=1,
        )

        escolha = st.selectbox("Selecione", df_sel["Rotulo"].tolist())
        confirmar = st.checkbox("Confirmo a exclusão definitiva", value=False)

        if st.button("Excluir selecionado") and confirmar:
            id_escolhido = escolha.split("ID=")[-1].strip()
            df_new = df.loc[df["ID"].astype(str) != id_escolhido].copy()
            salvar_excel(df_new, metas, ARQUIVO)
            st.cache_data.clear()
            st.success("Lançamento excluído.")
            st.rerun()

# -----------------------------
# Backup / Restore (sob demanda)
# -----------------------------
else:
    st.subheader("Backup / Restore")

    st.write("O backup em Excel pode ser pesado. Aqui ele só é gerado quando você pedir.")

    if "backup_bytes" not in st.session_state:
        st.session_state["backup_bytes"] = None

    if st.button("Gerar backup Excel"):
        st.session_state["backup_bytes"] = excel_bytes(df, metas)

    if st.session_state["backup_bytes"]:
        st.download_button(
            "Baixar Excel atualizado (backup)",
            data=st.session_state["backup_bytes"],
            file_name="controle_financeiro_casal_backup.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()
    st.subheader("Restore (restaurar backup)")

    up = st.file_uploader("Enviar backup (.xlsx)", type=["xlsx"])
    confirm_restore = st.checkbox("Confirmo que quero restaurar (substitui os dados atuais)", value=False)

    if st.button("Restaurar backup", type="primary") and up is not None and confirm_restore:
        try:
            df, metas = restore_from_upload(up)
            st.cache_data.clear()
            st.success("Backup restaurado.")
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao restaurar backup: {e}")
