import os
import io
import uuid
import calendar
import pandas as pd
import streamlit as st
from datetime import date, datetime

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

# (NOVO) colunas extras para suportar origem/conta fixa sem quebrar o resto
GASTOS_COLS = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs", "Origem", "RefFixa"]
METAS_COLS = ["Categoria", "Meta"]
FIXAS_COLS = ["ID_Fixa", "Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]

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

    # tipos
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

    for c in ["Categoria", "Subcategoria", "Pagamento", "Quem", "Obs", "Origem", "RefFixa"]:
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

    # garante existência de "Geral"
    if not (metas["Categoria"].str.lower() == "geral").any():
        metas = pd.concat([metas, pd.DataFrame([{"Categoria": "Geral", "Meta": 0}])], ignore_index=True)
        changed = True

    return metas[METAS_COLS].copy(), changed

def _ensure_fixas_schema(fixas: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    changed = False
    fixas = _normalizar_colunas(fixas)

    for c in FIXAS_COLS:
        if c not in fixas.columns:
            fixas[c] = pd.NA
            changed = True

    fixas["ID_Fixa"] = fixas["ID_Fixa"].astype("string").fillna("").astype(str)
    faltando = fixas["ID_Fixa"].str.strip() == ""
    if faltando.any():
        fixas.loc[faltando, "ID_Fixa"] = [uuid.uuid4().hex for _ in range(int(faltando.sum()))]
        changed = True

    fixas["Descricao"] = fixas["Descricao"].astype("string").fillna("").astype(str)
    fixas["Categoria"] = fixas["Categoria"].astype("string").fillna("").astype(str).str.strip()
    fixas["Valor"] = pd.to_numeric(fixas["Valor"], errors="coerce").fillna(0.0)
    fixas["Dia_Venc"] = pd.to_numeric(fixas["Dia_Venc"], errors="coerce").fillna(1).astype(int)

    fixas["Pagamento"] = fixas["Pagamento"].astype("string").fillna("").astype(str)
    fixas["Quem"] = fixas["Quem"].astype("string").fillna("").astype(str)

    # Ativo como boolean coerente
    if fixas["Ativo"].dtype != bool:
        fixas["Ativo"] = fixas["Ativo"].astype(str).str.strip().str.lower().isin(["true", "1", "sim", "yes", "y"])
        changed = True

    fixas["Obs"] = fixas["Obs"].astype("string").fillna("").astype(str)

    return fixas[FIXAS_COLS].copy(), changed

def salvar_excel(df_gastos: pd.DataFrame, df_metas: pd.DataFrame, df_fixas: pd.DataFrame, arquivo: str = ARQUIVO):
    g, _ = _ensure_gastos_schema(df_gastos)
    m, _ = _ensure_metas_schema(df_metas)
    f, _ = _ensure_fixas_schema(df_fixas)

    with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
        g.to_excel(writer, sheet_name="gastos", index=False)
        m.to_excel(writer, sheet_name="metas", index=False)
        f.to_excel(writer, sheet_name="fixas", index=False)

def init_arquivo_se_faltar():
    if os.path.exists(ARQUIVO):
        return
    df_vazio = pd.DataFrame(columns=GASTOS_COLS)
    metas_padrao = pd.DataFrame(
        {"Categoria": ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros", "Geral"],
         "Meta": [0, 0, 0, 0, 0, 0]}
    )
    fixas_vazio = pd.DataFrame(columns=FIXAS_COLS)
    salvar_excel(df_vazio, metas_padrao, fixas_vazio, ARQUIVO)

@st.cache_data(show_spinner=False)
def carregar_excel_cached(path: str, mtime: float):
    xls = pd.ExcelFile(path)
    sheets = set(xls.sheet_names)

    gastos = pd.read_excel(xls, sheet_name="gastos") if "gastos" in sheets else pd.DataFrame(columns=GASTOS_COLS)
    metas = pd.read_excel(xls, sheet_name="metas") if "metas" in sheets else pd.DataFrame(columns=METAS_COLS)
    fixas = pd.read_excel(xls, sheet_name="fixas") if "fixas" in sheets else pd.DataFrame(columns=FIXAS_COLS)

    return gastos, metas, fixas

def carregar_excel():
    init_arquivo_se_faltar()
    mtime = os.path.getmtime(ARQUIVO)
    gastos_raw, metas_raw, fixas_raw = carregar_excel_cached(ARQUIVO, mtime)

    g, chg_g = _ensure_gastos_schema(gastos_raw)
    m, chg_m = _ensure_metas_schema(metas_raw)
    f, chg_f = _ensure_fixas_schema(fixas_raw)

    if chg_g or chg_m or chg_f:
        salvar_excel(g, m, f, ARQUIVO)
        st.cache_data.clear()
        st.rerun()

    return g, m, f

def excel_bytes(g: pd.DataFrame, m: pd.DataFrame, f: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        g.to_excel(writer, sheet_name="gastos", index=False)
        m.to_excel(writer, sheet_name="metas", index=False)
        f.to_excel(writer, sheet_name="fixas", index=False)
    return buffer.getvalue()

def filtro_periodo_gastos(df: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    dfx = df.copy()
    dfx["Data_dt"] = pd.to_datetime(dfx["Data"], errors="coerce")
    mask = (dfx["Data_dt"].dt.year == ano) & (dfx["Data_dt"].dt.month == mes)
    out = dfx.loc[mask].copy()
    out.drop(columns=["Data_dt"], inplace=True, errors="ignore")
    out, _ = _ensure_gastos_schema(out)
    return out

def restore_from_upload(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    xls = pd.ExcelFile(uploaded_file)
    sheets = set(xls.sheet_names)

    g = pd.read_excel(xls, sheet_name="gastos") if "gastos" in sheets else pd.DataFrame(columns=GASTOS_COLS)
    m = pd.read_excel(xls, sheet_name="metas") if "metas" in sheets else pd.DataFrame(columns=METAS_COLS)
    f = pd.read_excel(xls, sheet_name="fixas") if "fixas" in sheets else pd.DataFrame(columns=FIXAS_COLS)

    g, _ = _ensure_gastos_schema(g)
    m, _ = _ensure_metas_schema(m)
    f, _ = _ensure_fixas_schema(f)

    salvar_excel(g, m, f, ARQUIVO)
    return g, m, f

def ultimo_dia_mes(ano: int, mes: int) -> int:
    return calendar.monthrange(ano, mes)[1]

def gerar_lancamentos_fixas(df_gastos: pd.DataFrame, df_fixas: pd.DataFrame, ano: int, mes: int) -> tuple[pd.DataFrame, int, int]:
    """
    Cria lançamentos no 'gastos' para cada conta fixa ativa no mês informado.
    Evita duplicidade por (RefFixa, ano, mes, Origem='FIXA').
    Retorna (df_atualizado, criados, ignorados).
    """
    g = df_gastos.copy()
    f = df_fixas.copy()

    # base de duplicidade
    g["Data_dt"] = pd.to_datetime(g["Data"], errors="coerce")
    ja = g[(g["Origem"].astype(str) == "FIXA") & (g["Data_dt"].dt.year == ano) & (g["Data_dt"].dt.month == mes)]
    ja_keys = set((ja["RefFixa"].astype(str)).tolist())

    criados = 0
    ignorados = 0

    ld = ultimo_dia_mes(ano, mes)

    for _, row in f.iterrows():
        if not bool(row.get("Ativo", False)):
            continue

        ref = str(row.get("ID_Fixa", "")).strip()
        if not ref:
            continue

        if ref in ja_keys:
            ignorados += 1
            continue

        dia = int(row.get("Dia_Venc", 1))
        dia = max(1, min(dia, ld))
        data_lcto = date(ano, mes, dia)

        desc = str(row.get("Descricao", "")).strip()
        cat = str(row.get("Categoria", "")).strip()
        val = float(row.get("Valor", 0.0))
        pag = str(row.get("Pagamento", "")).strip() or "PIX"
        quem = str(row.get("Quem", "")).strip() or PESSOAS[0]
        obs = str(row.get("Obs", "")).strip()

        novo = {
            "ID": uuid.uuid4().hex,
            "Data": data_lcto,
            "Categoria": cat if cat else "Outros",
            "Subcategoria": desc if desc else "Conta fixa",
            "Valor": val,
            "Pagamento": pag,
            "Quem": quem,
            "Obs": (f"Conta fixa: {desc}" if desc else "Conta fixa") + (f" | {obs}" if obs else ""),
            "Origem": "FIXA",
            "RefFixa": ref,
        }

        g = pd.concat([g, pd.DataFrame([novo])], ignore_index=True)
        criados += 1

    g.drop(columns=["Data_dt"], inplace=True, errors="ignore")
    g, _ = _ensure_gastos_schema(g)
    return g, criados, ignorados

# -----------------------------
# APP
# -----------------------------
df_gastos, df_metas, df_fixas = carregar_excel()

# categorias a partir das metas
cats = df_metas["Categoria"].dropna().astype(str).str.strip().tolist()
cats_lanc = [c for c in cats if c.lower() != "geral" and c != ""]
if not cats_lanc:
    cats_lanc = ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros"]

# anos disponíveis
hoje = date.today()
tmp = df_gastos.copy()
tmp["Data_dt"] = pd.to_datetime(tmp["Data"], errors="coerce")
anos = sorted([int(a) for a in tmp["Data_dt"].dt.year.dropna().unique().tolist()])
if hoje.year not in anos:
    anos = sorted(list(set(anos + [hoje.year])))

with st.sidebar:
    menu = st.radio("Menu", ["Lançar", "Resumo", "Gerenciar", "Metas", "Contas Fixas", "Backup/Restore"], index=0)

    st.divider()
    st.subheader("Período")
    ano_sel = st.selectbox("Ano", anos, index=anos.index(hoje.year))
    mes_sel = st.selectbox("Mês", [m for m, _ in MESES], index=hoje.month - 1, format_func=lambda m: MES_NOME[m])

st.title("Controle Financeiro do Casal")

# -----------------------------
# LANÇAR
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
            "Origem": "",
            "RefFixa": "",
        }
        df_gastos = pd.concat([df_gastos, pd.DataFrame([novo])], ignore_index=True)
        salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("Últimos lançamentos")
    show = df_gastos.copy()
    show["Data_dt"] = pd.to_datetime(show["Data"], errors="coerce")
    st.dataframe(
        show.sort_values("Data_dt", ascending=False)[["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]].head(30),
        use_container_width=True
    )

# -----------------------------
# RESUMO
# -----------------------------
elif menu == "Resumo":
    st.subheader(f"Resumo: {MES_NOME[mes_sel]}/{ano_sel}")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)
    total_lancado = float(df_periodo["Valor"].sum()) if not df_periodo.empty else 0.0

    # contas fixas previstas (ativas)
    fixas_ativas = df_fixas[df_fixas["Ativo"] == True].copy()
    total_fixas_previsto = float(fixas_ativas["Valor"].sum()) if not fixas_ativas.empty else 0.0
    total_previsto = total_lancado + total_fixas_previsto

    c1, c2, c3 = st.columns(3)
    c1.metric("Gasto lançado no mês", f"R$ {total_lancado:,.2f}")
    c2.metric("Contas fixas previstas", f"R$ {total_fixas_previsto:,.2f}")
    c3.metric("Total previsto (lançado + fixas)", f"R$ {total_previsto:,.2f}")

    # meta geral
    meta_geral = df_metas[df_metas["Categoria"].str.lower() == "geral"]["Meta"]
    meta_geral_val = float(meta_geral.iloc[0]) if len(meta_geral) > 0 else 0.0

    st.divider()
    st.subheader("Metas do mês")

    if meta_geral_val > 0:
        progresso = min(total_previsto / meta_geral_val, 1.0)
        st.progress(progresso)
        st.write(f"Meta geral: R$ {total_previsto:,.2f} / R$ {meta_geral_val:,.2f}")
        if total_previsto > meta_geral_val:
            st.warning("Meta geral ultrapassada (considerando fixas previstas).")
    else:
        st.info("Defina a meta geral na aba 'Metas' (categoria 'Geral').")

    # resumo por categoria (lançado)
    st.divider()
    st.subheader("Gastos lançados por categoria (mês)")
    if df_periodo.empty:
        st.write("Sem lançamentos neste período.")
    else:
        resumo_cat = (
            df_periodo.groupby("Categoria")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        st.dataframe(resumo_cat, use_container_width=True)

    # metas por categoria (comparação contra lançado)
    st.subheader("Acompanhamento de metas por categoria (lançado)")
    metas_map = df_metas.set_index(df_metas["Categoria"].astype(str).str.strip())["Meta"].to_dict()

    # mostra somente categorias com meta > 0 (exceto Geral)
    metas_cat = df_metas[(df_metas["Categoria"].str.lower() != "geral") & (df_metas["Meta"] > 0)].copy()
    if metas_cat.empty:
        st.info("Nenhuma meta por categoria definida (além de Geral).")
    else:
        gasto_por_cat = {}
        if not df_periodo.empty:
            gasto_por_cat = df_periodo.groupby("Categoria")["Valor"].sum().to_dict()

        for _, r in metas_cat.iterrows():
            cat = str(r["Categoria"]).strip()
            meta = float(r["Meta"])
            gasto = float(gasto_por_cat.get(cat, 0.0))
            prog = min(gasto / meta, 1.0) if meta > 0 else 0.0
            st.write(cat)
            st.progress(prog)
            st.write(f"R$ {gasto:,.2f} / R$ {meta:,.2f}")
            if gasto > meta:
                st.warning(f"Meta ultrapassada em {cat}.")

# -----------------------------
# GERENCIAR (EDITAR/APAGAR)
# -----------------------------
elif menu == "Gerenciar":
    st.subheader("Gerenciar lançamentos (editar / apagar)")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)

    col1, col2 = st.columns([1, 1])
    with col1:
        editar_todos = st.checkbox("Editar todos (não só o período)", value=False)
    with col2:
        mostrar_colunas_tecnicas = st.checkbox("Mostrar colunas técnicas (Origem/RefFixa)", value=False)

    df_view = df_gastos.copy() if editar_todos else df_periodo.copy()

    if df_view.empty:
        st.info("Sem lançamentos para este filtro.")
    else:
        base_cols = ["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]
        tech_cols = ["Origem", "RefFixa"]
        cols_show = ["ID"] + base_cols + (tech_cols if mostrar_colunas_tecnicas else [])

        df_editor_base = df_view.copy()
        df_editor_base["Data"] = pd.to_datetime(df_editor_base["Data"], errors="coerce").dt.strftime("%Y-%m-%d")

        edited = st.data_editor(
            df_editor_base[cols_show],
            num_rows="dynamic",
            use_container_width=True,
            key=f"editor_{'ALL' if editar_todos else 'PER'}_{ano_sel}_{mes_sel}_{mostrar_colunas_tecnicas}",
        )

        salvar_alt = st.button("Salvar alterações", type="primary")

        if salvar_alt:
            try:
                # garante colunas completas
                for c in GASTOS_COLS:
                    if c not in edited.columns:
                        edited[c] = ""

                edited = edited[GASTOS_COLS].copy()
                edited["Data"] = pd.to_datetime(edited["Data"], errors="coerce").dt.date
                edited["Valor"] = pd.to_numeric(edited["Valor"], errors="coerce").fillna(0.0)

                base = df_gastos.copy()
                base, _ = _ensure_gastos_schema(base)

                edited_ids = set(edited["ID"].astype(str).tolist())
                base_keep = base.loc[~base["ID"].astype(str).isin(edited_ids)].copy()

                final_df = pd.concat([base_keep, edited], ignore_index=True)
                final_df, _ = _ensure_gastos_schema(final_df)

                df_gastos = final_df
                salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar alterações: {e}")

        st.divider()
        st.subheader("Excluir lançamento rápido")
        df_sel = df_view.copy()
        df_sel["DataStr"] = pd.to_datetime(df_sel["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
        df_sel["Rotulo"] = df_sel.apply(
            lambda r: f"{r['DataStr']} | {r['Categoria']} | R$ {float(r['Valor']):,.2f} | {r['Quem']} | ID={r['ID']}",
            axis=1,
        )
        escolha = st.selectbox("Selecione", df_sel["Rotulo"].tolist())
        confirmar = st.checkbox("Confirmo a exclusão definitiva", value=False)

        if st.button("Excluir selecionado") and confirmar:
            id_escolhido = escolha.split("ID=")[-1].strip()
            df_gastos = df_gastos.loc[df_gastos["ID"].astype(str) != id_escolhido].copy()
            salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

# -----------------------------
# METAS
# -----------------------------
elif menu == "Metas":
    st.subheader("Metas (por categoria e geral)")

    st.write("Edite a tabela abaixo. A categoria 'Geral' é usada como meta total do mês.")
    metas_edit = st.data_editor(df_metas, num_rows="dynamic", use_container_width=True, key="metas_editor")

    if st.button("Salvar metas", type="primary"):
        try:
            metas_edit, _ = _ensure_metas_schema(metas_edit)
            df_metas = metas_edit
            salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar metas: {e}")

    st.divider()
    st.subheader("Dica de uso")
    st.write("Se quiser controlar categorias novas no app, adicione a categoria aqui e defina uma meta (mesmo que seja 0).")

# -----------------------------
# CONTAS FIXAS
# -----------------------------
elif menu == "Contas Fixas":
    st.subheader("Contas fixas")

    st.write("Cadastre contas recorrentes (aluguel, internet, energia, escola, etc.).")
    st.divider()

    with st.form("nova_fixa"):
        desc = st.text_input("Descrição (ex.: Aluguel, Internet)")
        cat = st.selectbox("Categoria", cats_lanc + ["Outros"])
        val = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
        dia = st.number_input("Dia de vencimento (1 a 31)", min_value=1, max_value=31, step=1, value=5)
        pag = st.selectbox("Forma de pagamento", PAGAMENTOS)
        quem = st.selectbox("Responsável", PESSOAS)
        ativo = st.checkbox("Ativo", value=True)
        obs = st.text_input("Observação")
        add = st.form_submit_button("Adicionar conta fixa")

    if add:
        novo = {
            "ID_Fixa": uuid.uuid4().hex,
            "Descricao": desc.strip(),
            "Categoria": cat.strip(),
            "Valor": float(val),
            "Dia_Venc": int(dia),
            "Pagamento": pag,
            "Quem": quem,
            "Ativo": bool(ativo),
            "Obs": obs,
        }
        df_fixas = pd.concat([df_fixas, pd.DataFrame([novo])], ignore_index=True)
        salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("Editar / apagar contas fixas")

    fixas_edit = st.data_editor(df_fixas, num_rows="dynamic", use_container_width=True, key="fixas_editor")

    c1, c2 = st.columns([1, 2])
    with c1:
        salvar_fixas = st.button("Salvar contas fixas", type="primary")
    with c2:
        st.caption("Para apagar: remova a linha na tabela e clique em salvar.")

    if salvar_fixas:
        try:
            fixas_edit, _ = _ensure_fixas_schema(fixas_edit)
            df_fixas = fixas_edit
            salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar contas fixas: {e}")

    st.divider()
    st.subheader(f"Gerar lançamentos das contas fixas em {MES_NOME[mes_sel]}/{ano_sel}")

    st.write("Isto cria lançamentos no histórico de gastos para as contas fixas ativas, evitando duplicidade no mês.")
    if st.button("Gerar lançamentos do mês", type="primary"):
        try:
            df_gastos_novo, criados, ignorados = gerar_lancamentos_fixas(df_gastos, df_fixas, ano_sel, mes_sel)
            df_gastos = df_gastos_novo
            salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
            st.cache_data.clear()
            st.success(f"Lançamentos criados: {criados}. Ignorados (já existiam): {ignorados}.")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao gerar lançamentos: {e}")

# -----------------------------
# BACKUP / RESTORE
# -----------------------------
else:
    st.subheader("Backup / Restore")

    if "backup_bytes" not in st.session_state:
        st.session_state["backup_bytes"] = None

    if st.button("Gerar backup Excel"):
        st.session_state["backup_bytes"] = excel_bytes(df_gastos, df_metas, df_fixas)

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
            df_gastos, df_metas, df_fixas = restore_from_upload(up)
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao restaurar backup: {e}")
