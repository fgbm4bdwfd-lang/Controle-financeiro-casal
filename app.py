import os
import io
import uuid
import time
import shutil
import calendar
import pandas as pd
import streamlit as st
from datetime import date

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(page_title="Controle Financeiro", layout="centered")

ARQUIVO = "dados.xlsx"
LOCKFILE = f"{ARQUIVO}.lock"

PAGAMENTOS = ["PIX", "Cartão Pão de Açucar", "Cartão Nubank", "Swile", "Pluxee"]
PESSOAS = ["Roney", "Adriele"]

MESES = [
    (1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"),
    (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"),
    (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro"),
]
MES_NOME = {n: nome for n, nome in MESES}

# Colunas
GASTOS_COLS = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs", "Origem", "RefFixa"]
METAS_COLS = ["Categoria", "Meta"]
FIXAS_COLS = ["ID_Fixa", "Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]


# -----------------------------
# LOCK (evita corrupção por escrita concorrente)
# -----------------------------
def acquire_lock(lock_path: str, stale_seconds: int = 180, timeout_seconds: int = 5) -> bool:
    start = time.time()
    while True:
        try:
            # se lock existir e for muito antigo, remove
            if os.path.exists(lock_path):
                try:
                    age = time.time() - os.path.getmtime(lock_path)
                    if age > stale_seconds:
                        try:
                            os.remove(lock_path)
                        except Exception:
                            pass
                except Exception:
                    pass

            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            if time.time() - start > timeout_seconds:
                return False
            time.sleep(0.12)
        except Exception:
            return False


def release_lock(lock_path: str):
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception:
        pass


# -----------------------------
# FUNÇÕES DE SCHEMA
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

    df["ID"] = df["ID"].astype("string")
    faltando = df["ID"].isna() | (df["ID"].str.strip() == "")
    if faltando.any():
        df.loc[faltando, "ID"] = [uuid.uuid4().hex for _ in range(int(faltando.sum()))]
        changed = True

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

    # garante "Geral"
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

    if fixas["Ativo"].dtype != bool:
        fixas["Ativo"] = fixas["Ativo"].astype(str).str.strip().str.lower().isin(["true", "1", "sim", "yes", "y"])
        changed = True

    fixas["Obs"] = fixas["Obs"].astype("string").fillna("").astype(str)

    return fixas[FIXAS_COLS].copy(), changed


def _default_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    g = pd.DataFrame(columns=GASTOS_COLS)
    m = pd.DataFrame(
        {"Categoria": ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros", "Geral"],
         "Meta": [0, 0, 0, 0, 0, 0]}
    )
    f = pd.DataFrame(columns=FIXAS_COLS)
    g, _ = _ensure_gastos_schema(g)
    m, _ = _ensure_metas_schema(m)
    f, _ = _ensure_fixas_schema(f)
    return g, m, f


# -----------------------------
# EXCEL: escrita atômica
# -----------------------------
def _write_bytes_atomic(path: str, data: bytes):
    tmp_path = f"{path}.tmp"
    bak_path = f"{path}.bak"

    if os.path.exists(path):
        try:
            shutil.copy2(path, bak_path)
        except Exception:
            pass

    with open(tmp_path, "wb") as f:
        f.write(data)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass

    os.replace(tmp_path, path)


def excel_bytes(g: pd.DataFrame, m: pd.DataFrame, f: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        g.to_excel(writer, sheet_name="gastos", index=False)
        m.to_excel(writer, sheet_name="metas", index=False)
        f.to_excel(writer, sheet_name="fixas", index=False)
    return buffer.getvalue()


def salvar_excel(g: pd.DataFrame, m: pd.DataFrame, f: pd.DataFrame, arquivo: str = ARQUIVO):
    ok = acquire_lock(LOCKFILE)
    if not ok:
        raise RuntimeError("Não foi possível obter lock para salvar. Tente novamente.")

    try:
        g2, _ = _ensure_gastos_schema(g)
        m2, _ = _ensure_metas_schema(m)
        f2, _ = _ensure_fixas_schema(f)

        data = excel_bytes(g2, m2, f2)
        _write_bytes_atomic(arquivo, data)
    finally:
        release_lock(LOCKFILE)


def init_arquivo_se_faltar():
    if os.path.exists(ARQUIVO):
        return
    g, m, f = _default_frames()
    salvar_excel(g, m, f, ARQUIVO)


@st.cache_data(show_spinner=False)
def carregar_excel_cached(path: str, mtime: float):
    try:
        xls = pd.ExcelFile(path)
        sheets = set(xls.sheet_names)

        gastos = pd.read_excel(xls, sheet_name="gastos") if "gastos" in sheets else pd.DataFrame(columns=GASTOS_COLS)
        metas = pd.read_excel(xls, sheet_name="metas") if "metas" in sheets else pd.DataFrame(columns=METAS_COLS)
        fixas = pd.read_excel(xls, sheet_name="fixas") if "fixas" in sheets else pd.DataFrame(columns=FIXAS_COLS)

        return {"ok": True, "gastos": gastos, "metas": metas, "fixas": fixas, "error": ""}
    except Exception as e:
        return {"ok": False, "gastos": None, "metas": None, "fixas": None, "error": repr(e)}


def _quarentena_arquivo(path: str) -> str:
    ts = time.strftime("%Y%m%d-%H%M%S")
    new_name = f"{path}.CORROMPIDO.{ts}"
    try:
        os.replace(path, new_name)
    except Exception:
        try:
            shutil.copy2(path, new_name)
            os.remove(path)
        except Exception:
            pass
    return new_name


def carregar_excel():
    init_arquivo_se_faltar()

    try:
        mtime = os.path.getmtime(ARQUIVO)
    except Exception:
        mtime = time.time()

    res = carregar_excel_cached(ARQUIVO, mtime)

    if not res["ok"]:
        corrompido = _quarentena_arquivo(ARQUIVO)
        g, m, f = _default_frames()
        salvar_excel(g, m, f, ARQUIVO)
        st.cache_data.clear()
        st.session_state["RECOVERY_MSG"] = (
            "O arquivo dados.xlsx no servidor estava corrompido e foi substituído por um novo. "
            f"O arquivo antigo foi movido para: {corrompido}. "
            "Vá em Backup/Restore e envie seu último backup para restaurar."
        )
        return g, m, f

    g, chg_g = _ensure_gastos_schema(res["gastos"])
    m, chg_m = _ensure_metas_schema(res["metas"])
    f, chg_f = _ensure_fixas_schema(res["fixas"])

    if chg_g or chg_m or chg_f:
        salvar_excel(g, m, f, ARQUIVO)
        st.cache_data.clear()
        return g, m, f

    return g, m, f


# -----------------------------
# HELPERS
# -----------------------------
def filtro_periodo_gastos(df: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    dfx = df.copy()
    dfx["Data_dt"] = pd.to_datetime(dfx["Data"], errors="coerce")
    mask = (dfx["Data_dt"].dt.year == ano) & (dfx["Data_dt"].dt.month == mes)
    out = dfx.loc[mask].copy()
    out.drop(columns=["Data_dt"], inplace=True, errors="ignore")
    out, _ = _ensure_gastos_schema(out)
    return out


def ultimo_dia_mes(ano: int, mes: int) -> int:
    return calendar.monthrange(ano, mes)[1]


def gerar_lancamentos_fixas(df_gastos: pd.DataFrame, df_fixas: pd.DataFrame, ano: int, mes: int) -> tuple[pd.DataFrame, int, int]:
    g = df_gastos.copy()
    f = df_fixas.copy()

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


def restore_from_upload(uploaded_file) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        xls = pd.ExcelFile(uploaded_file)
        sheets = set(xls.sheet_names)
    except Exception as e:
        raise ValueError(f"Arquivo enviado não parece ser um Excel válido: {e}")

    g = pd.read_excel(xls, sheet_name="gastos") if "gastos" in sheets else pd.DataFrame(columns=GASTOS_COLS)
    m = pd.read_excel(xls, sheet_name="metas") if "metas" in sheets else pd.DataFrame(columns=METAS_COLS)
    f = pd.read_excel(xls, sheet_name="fixas") if "fixas" in sheets else pd.DataFrame(columns=FIXAS_COLS)

    g, _ = _ensure_gastos_schema(g)
    m, _ = _ensure_metas_schema(m)
    f, _ = _ensure_fixas_schema(f)

    salvar_excel(g, m, f, ARQUIVO)
    st.cache_data.clear()
    return g, m, f


# -----------------------------
# APP
# -----------------------------
df_gastos, df_metas, df_fixas = carregar_excel()

if "RECOVERY_MSG" in st.session_state:
    st.warning(st.session_state["RECOVERY_MSG"])

# categorias
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
    menu = st.radio(
        "Menu",
        ["Lançar", "Resumo", "Gerenciar", "Metas", "Contas Fixas", "Backup/Restore"],
        index=0
    )

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
        try:
            salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
        except Exception as e:
            st.error(str(e))
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
    gasto_mes = float(df_periodo["Valor"].sum()) if not df_periodo.empty else 0.0

    fixas_ativas = df_fixas[df_fixas["Ativo"] == True].copy()
    fixas_prev = float(fixas_ativas["Valor"].sum()) if not fixas_ativas.empty else 0.0
    total_prev = gasto_mes + fixas_prev

    c1, c2, c3 = st.columns(3)
    c1.metric("Gasto lançado no mês", f"R$ {gasto_mes:,.2f}")
    c2.metric("Fixas previstas (ativas)", f"R$ {fixas_prev:,.2f}")
    c3.metric("Total previsto (mês)", f"R$ {total_prev:,.2f}")

    st.divider()
    st.subheader("Por categoria (lançado)")
    if df_periodo.empty:
        st.info("Sem lançamentos no período.")
    else:
        resumo_cat = (
            df_periodo.groupby("Categoria")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )

        total_cat = float(resumo_cat["Valor"].sum())
        if total_cat > 0:
            resumo_cat["Percentual"] = (resumo_cat["Valor"] / total_cat) * 100.0
        else:
            resumo_cat["Percentual"] = 0.0

        resumo_cat_view = resumo_cat.copy()
        resumo_cat_view["Percentual"] = resumo_cat_view["Percentual"].map(lambda x: f"{x:.2f}%")

        st.dataframe(
            resumo_cat_view[["Categoria", "Valor", "Percentual"]],
            use_container_width=True
        )

    st.subheader("Por pessoa (lançado)")
    if not df_periodo.empty:
        resumo_pessoa = (
            df_periodo.groupby("Quem")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        st.dataframe(resumo_pessoa, use_container_width=True)


# -----------------------------
# GERENCIAR
# -----------------------------
elif menu == "Gerenciar":
    st.subheader("Gerenciar lançamentos (editar / apagar)")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)

    c1, c2 = st.columns([1, 1])
    with c1:
        editar_todos = st.checkbox("Editar todos (não só o período)", value=False)
    with c2:
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

        if st.button("Salvar alterações", type="primary"):
            try:
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
            try:
                id_escolhido = escolha.split("ID=")[-1].strip()
                df_gastos = df_gastos.loc[df_gastos["ID"].astype(str) != id_escolhido].copy()
                salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao excluir: {e}")


# -----------------------------
# METAS (com porcentagem, gasto e falta)
# -----------------------------
elif menu == "Metas":
    st.subheader("Metas")

    st.write("Edite as metas abaixo. A categoria 'Geral' é a meta total do mês.")
    metas_edit = st.data_editor(
        df_metas,
        num_rows="dynamic",
        use_container_width=True,
        key="metas_editor"
    )

    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Salvar metas", type="primary"):
            try:
                metas_edit, _ = _ensure_metas_schema(metas_edit)
                df_metas = metas_edit
                salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar metas: {e}")

    with col_info:
        st.caption("O acompanhamento abaixo considera o mês/ano selecionado na barra lateral.")

    st.divider()
    st.subheader(f"Acompanhamento: {MES_NOME[mes_sel]}/{ano_sel}")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)
    gasto_total_mes = float(df_periodo["Valor"].sum()) if not df_periodo.empty else 0.0

    fixas_ativas = df_fixas[df_fixas["Ativo"] == True].copy()
    total_fixas_previsto = float(fixas_ativas["Valor"].sum()) if not fixas_ativas.empty else 0.0
    total_previsto = gasto_total_mes + total_fixas_previsto

    metas_base = metas_edit.copy()
    metas_base["Categoria"] = metas_base["Categoria"].astype(str).str.strip()
    metas_base["Meta"] = pd.to_numeric(metas_base["Meta"], errors="coerce").fillna(0.0)

    meta_geral_series = metas_base[metas_base["Categoria"].str.lower() == "geral"]["Meta"]
    meta_geral = float(meta_geral_series.iloc[0]) if len(meta_geral_series) > 0 else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Gasto lançado no mês", f"R$ {gasto_total_mes:,.2f}")
    c2.metric("Fixas previstas (ativas)", f"R$ {total_fixas_previsto:,.2f}")
    c3.metric("Total previsto (mês)", f"R$ {total_previsto:,.2f}")

    st.divider()
    st.subheader("Meta Geral (mês)")

    if meta_geral > 0:
        perc_geral = (total_previsto / meta_geral) * 100.0
        falta_geral = max(meta_geral - total_previsto, 0.0)
        excedeu_geral = max(total_previsto - meta_geral, 0.0)

        st.write(f"Meta: R$ {meta_geral:,.2f}")
        st.write(f"Usado: R$ {total_previsto:,.2f} | Falta: R$ {falta_geral:,.2f} | Excedeu: R$ {excedeu_geral:,.2f}")
        st.progress(min(total_previsto / meta_geral, 1.0))
        st.write(f"Percentual usado: {perc_geral:.2f}%")
    else:
        st.info("Defina a meta 'Geral' (> 0) para ver o percentual geral do mês.")

    st.divider()
    st.subheader("Metas por categoria (gasto lançado no mês)")

    if df_periodo.empty:
        gasto_por_cat = {}
    else:
        gasto_por_cat = df_periodo.groupby("Categoria")["Valor"].sum().to_dict()

    metas_cat = metas_base[metas_base["Categoria"].str.lower() != "geral"].copy()
    metas_cat["Gasto_Mes"] = metas_cat["Categoria"].map(lambda c: float(gasto_por_cat.get(str(c).strip(), 0.0)))
    metas_cat["Falta"] = (metas_cat["Meta"] - metas_cat["Gasto_Mes"]).clip(lower=0.0)
    metas_cat["Excedeu"] = (metas_cat["Gasto_Mes"] - metas_cat["Meta"]).clip(lower=0.0)

    def _pct(row):
        meta = float(row["Meta"])
        gasto = float(row["Gasto_Mes"])
        if meta <= 0:
            return 0.0
        return (gasto / meta) * 100.0

    metas_cat["Percentual_Usado"] = metas_cat.apply(_pct, axis=1)

    view = metas_cat[["Categoria", "Meta", "Gasto_Mes", "Falta", "Excedeu", "Percentual_Usado"]].copy()
    view = view.sort_values(["Percentual_Usado", "Gasto_Mes"], ascending=[False, False])

    st.dataframe(view, use_container_width=True)

    st.divider()
    st.subheader("Progresso por categoria")
    for _, r in view.iterrows():
        cat = str(r["Categoria"])
        meta = float(r["Meta"])
        gasto = float(r["Gasto_Mes"])
        pct = float(r["Percentual_Usado"])
        if meta > 0:
            st.write(f"{cat} — R$ {gasto:,.2f} / R$ {meta:,.2f} ({pct:.2f}%)")
            st.progress(min(gasto / meta, 1.0))
        else:
            st.write(f"{cat} — Meta não definida (0). Gasto no mês: R$ {gasto:,.2f}")


# -----------------------------
# CONTAS FIXAS
# -----------------------------
elif menu == "Contas Fixas":
    st.subheader("Contas fixas")

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
        try:
            salvar_excel(df_gastos, df_metas, df_fixas, ARQUIVO)
        except Exception as e:
            st.error(str(e))
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("Editar / apagar contas fixas")
    fixas_edit = st.data_editor(df_fixas, num_rows="dynamic", use_container_width=True, key="fixas_editor")

    if st.button("Salvar contas fixas", type="primary"):
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
    st.caption("Cria lançamentos em 'gastos' para as fixas ativas, sem duplicar no mês.")

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
        try:
            st.session_state["backup_bytes"] = excel_bytes(df_gastos, df_metas, df_fixas)
        except Exception as e:
            st.error(f"Erro ao gerar backup: {e}")

    if st.session_state["backup_bytes"]:
        st.download_button(
            "Baixar Excel atualizado (backup)",
            data=st.session_state["backup_bytes"],
            file_name="controle_financeiro_casal_backup.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()
    st.subheader("Restore")
    up = st.file_uploader("Enviar backup (.xlsx)", type=["xlsx"])
    confirm_restore = st.checkbox("Confirmo que quero restaurar (substitui os dados atuais)", value=False)

    if st.button("Restaurar backup", type="primary") and up is not None and confirm_restore:
        try:
            df_gastos, df_metas, df_fixas = restore_from_upload(up)
            st.success("Backup restaurado.")
            st.rerun()
        except Exception as e:
            st.error(f"Falha ao restaurar backup: {e}")
