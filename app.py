import os
import time
import uuid
import shutil
import tempfile
from datetime import date, datetime

import pandas as pd
import streamlit as st

ARQUIVO = "dados.xlsx"

PAGAMENTOS_PADRAO = [
    "PIX",
    "Cartão Pão de Açucar",
    "Cartão Nubank",
    "Swile",
    "Pluxee",
    "Boleto",
]

PESSOAS_PADRAO = ["Roney", "Adriele"]

MESES = [
    ("Janeiro", 1),
    ("Fevereiro", 2),
    ("Março", 3),
    ("Abril", 4),
    ("Maio", 5),
    ("Junho", 6),
    ("Julho", 7),
    ("Agosto", 8),
    ("Setembro", 9),
    ("Outubro", 10),
    ("Novembro", 11),
    ("Dezembro", 12),
]


# -----------------------------
# Formatação
# -----------------------------
def fmt_money(v) -> str:
    try:
        if pd.isna(v):
            v = 0.0
        v = float(v)
    except Exception:
        v = 0.0
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def fmt_pct(p) -> str:
    if p is None or (isinstance(p, float) and pd.isna(p)):
        return "—"
    try:
        p = float(p)
    except Exception:
        return "—"
    s = f"{p:.2f}".replace(".", ",")
    return f"{s}%"


def novo_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def periodo_inicio_fim(ano: int, mes: int):
    ini = datetime(ano, mes, 1)
    if mes == 12:
        fim = datetime(ano + 1, 1, 1)
    else:
        fim = datetime(ano, mes + 1, 1)
    return ini, fim


# -----------------------------
# Escrita segura (evita BadZipFile)
# -----------------------------
def acquire_lock(lock_path: str, timeout_s: int = 20) -> bool:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            time.sleep(0.2)
    return False


def release_lock(lock_path: str):
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception:
        pass


def salvar_excel_seguro(dfs: dict):
    lock_path = ARQUIVO + ".lock"
    ok = acquire_lock(lock_path, timeout_s=20)
    if not ok:
        st.error("Não consegui salvar agora (lock). Recarregue e tente novamente.")
        return

    try:
        tmp_dir = tempfile.mkdtemp()
        tmp_file = os.path.join(tmp_dir, "dados_tmp.xlsx")

        with pd.ExcelWriter(tmp_file, engine="openpyxl") as writer:
            for nome, df in dfs.items():
                df.to_excel(writer, sheet_name=nome, index=False)

        shutil.move(tmp_file, ARQUIVO)
        shutil.rmtree(tmp_dir, ignore_errors=True)
    finally:
        release_lock(lock_path)


def garantir_colunas(df: pd.DataFrame, cols_defaults: dict) -> pd.DataFrame:
    df = df.copy()
    for c, default in cols_defaults.items():
        if c not in df.columns:
            df[c] = default
    return df


def normalizar_reservas(df_reservas: pd.DataFrame) -> pd.DataFrame:
    """
    Corrige casos em que a aba reservas veio com colunas diferentes.
    Garante: ID_Reserva, Reserva, Meta, Reservado, Ativa
    """
    if df_reservas is None or df_reservas.empty:
        return pd.DataFrame(columns=["ID_Reserva", "Reserva", "Meta", "Reservado", "Ativa"])

    df = df_reservas.copy()

    # tenta mapear nomes parecidos
    rename_map = {}
    for col in df.columns:
        c = str(col).strip().lower()
        if c in ["id_reserva", "idreserva", "id"]:
            rename_map[col] = "ID_Reserva"
        elif c in ["reserva", "nome", "descricao", "descrição"]:
            rename_map[col] = "Reserva"
        elif c in ["meta", "objetivo", "meta_total"]:
            rename_map[col] = "Meta"
        elif c in ["reservado", "total_reservado", "valor_reservado", "acumulado"]:
            rename_map[col] = "Reservado"
        elif c in ["ativa", "ativo", "status"]:
            rename_map[col] = "Ativa"

    if rename_map:
        df = df.rename(columns=rename_map)

    df = garantir_colunas(
        df,
        {
            "ID_Reserva": "",
            "Reserva": "",
            "Meta": 0.0,
            "Reservado": 0.0,
            "Ativa": True,
        },
    )

    # ids vazios -> gera
    df["ID_Reserva"] = df["ID_Reserva"].fillna("").astype(str)
    for i in range(len(df)):
        if df.loc[i, "ID_Reserva"].strip() == "":
            df.loc[i, "ID_Reserva"] = novo_id("RES")

    # tipos
    df["Meta"] = pd.to_numeric(df["Meta"], errors="coerce").fillna(0)
    df["Reservado"] = pd.to_numeric(df["Reservado"], errors="coerce").fillna(0)
    df["Ativa"] = df["Ativa"].fillna(True).astype(bool)

    return df


def garantir_arquivo():
    if os.path.exists(ARQUIVO):
        return

    df_gastos = pd.DataFrame(
        columns=[
            "ID_Gasto",
            "Data",
            "Categoria",
            "Subcategoria",
            "Valor",
            "Pagamento",
            "Quem",
            "Obs",
            "Tipo",     # Variável / Fixa
            "ID_Fixa",  # referência
        ]
    )

    df_metas = pd.DataFrame(columns=["Categoria", "Meta"])

    df_fixas = pd.DataFrame(
        columns=[
            "ID_Fixa",
            "Descricao",
            "Categoria",
            "Valor",
            "Dia_Venc",
            "Pagamento",
            "Quem",
            "Ativa",
        ]
    )

    df_reservas = pd.DataFrame(
        [
            {"ID_Reserva": novo_id("RES"), "Reserva": "Emergência (6 meses)", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Saúde", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Manutenção carro", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Manutenção casa", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Viagens / Lazer", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
        ]
    )

    df_config = pd.DataFrame([{"Chave": "META_GERAL", "Valor": 0.0}])

    salvar_excel_seguro(
        {
            "gastos": df_gastos,
            "metas": df_metas,
            "fixas": df_fixas,
            "reservas": df_reservas,
            "config": df_config,
        }
    )


@st.cache_data(show_spinner=False)
def carregar_excel_cached(path: str, mtime: float):
    xls = pd.ExcelFile(path)
    gastos = pd.read_excel(xls, "gastos")
    metas = pd.read_excel(xls, "metas")
    fixas = pd.read_excel(xls, "fixas")
    reservas = pd.read_excel(xls, "reservas") if "reservas" in xls.sheet_names else pd.DataFrame()
    config = pd.read_excel(xls, "config") if "config" in xls.sheet_names else pd.DataFrame()
    return gastos, metas, fixas, reservas, config


def carregar_excel():
    garantir_arquivo()
    mtime = os.path.getmtime(ARQUIVO)
    df_gastos, df_metas, df_fixas, df_reservas, df_config = carregar_excel_cached(ARQUIVO, mtime)

    # gastos
    df_gastos = garantir_colunas(
        df_gastos,
        {
            "ID_Gasto": "",
            "Data": pd.NaT,
            "Categoria": "",
            "Subcategoria": "",
            "Valor": 0.0,
            "Pagamento": "",
            "Quem": "",
            "Obs": "",
            "Tipo": "Variável",
            "ID_Fixa": "",
        },
    )
    df_gastos["ID_Gasto"] = df_gastos["ID_Gasto"].fillna("").astype(str)
    for i in range(len(df_gastos)):
        if df_gastos.loc[i, "ID_Gasto"].strip() == "":
            df_gastos.loc[i, "ID_Gasto"] = novo_id("GAS")
    df_gastos["Data"] = pd.to_datetime(df_gastos["Data"], errors="coerce")
    df_gastos["Valor"] = pd.to_numeric(df_gastos["Valor"], errors="coerce").fillna(0)

    # metas
    df_metas = garantir_colunas(df_metas, {"Categoria": "", "Meta": 0.0})
    df_metas["Meta"] = pd.to_numeric(df_metas["Meta"], errors="coerce").fillna(0)

    # fixas
    df_fixas = garantir_colunas(
        df_fixas,
        {
            "ID_Fixa": "",
            "Descricao": "",
            "Categoria": "",
            "Valor": 0.0,
            "Dia_Venc": 1,
            "Pagamento": "PIX",
            "Quem": PESSOAS_PADRAO[0],
            "Ativa": True,
        },
    )
    df_fixas["ID_Fixa"] = df_fixas["ID_Fixa"].fillna("").astype(str)
    for i in range(len(df_fixas)):
        if df_fixas.loc[i, "ID_Fixa"].strip() == "":
            df_fixas.loc[i, "ID_Fixa"] = novo_id("FIX")
    df_fixas["Valor"] = pd.to_numeric(df_fixas["Valor"], errors="coerce").fillna(0)
    df_fixas["Dia_Venc"] = pd.to_numeric(df_fixas["Dia_Venc"], errors="coerce").fillna(1).astype(int)
    df_fixas["Ativa"] = df_fixas["Ativa"].fillna(True).astype(bool)

    # reservas (corrige KeyError Reservado)
    df_reservas = normalizar_reservas(df_reservas)

    # config
    if df_config is None or df_config.empty:
        df_config = pd.DataFrame([{"Chave": "META_GERAL", "Valor": 0.0}])
    df_config = garantir_colunas(df_config, {"Chave": "", "Valor": 0.0})
    df_config["Valor"] = pd.to_numeric(df_config["Valor"], errors="coerce").fillna(0)

    return df_gastos, df_metas, df_fixas, df_reservas, df_config


def salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config):
    salvar_excel_seguro(
        {
            "gastos": df_gastos,
            "metas": df_metas,
            "fixas": df_fixas,
            "reservas": df_reservas,
            "config": df_config,
        }
    )
    carregar_excel_cached.clear()


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Controle Financeiro do Casal", layout="wide")

# CSS: volta a ficar “limpo”, sem gigantismo
st.markdown(
    """
<style>
h1 {font-size: 2.1rem !important; margin-bottom: 0.5rem;}
h2 {font-size: 1.35rem !important;}
div[data-testid="stMetricValue"] {font-size: 1.55rem !important;}
div[data-testid="stMetricLabel"] {font-size: 0.90rem !important;}

.progress-row{
  display:flex;
  align-items:center;
  gap:12px;
  padding:6px 0;
}
.progress-label{
  flex:0 0 240px;
  font-weight:600;
  overflow:hidden;
  text-overflow:ellipsis;
  white-space:nowrap;
}
.progress-bar-wrap{
  flex: 1 1 auto;
  background: rgba(49,51,63,0.12);
  border-radius: 999px;
  height: 14px;
  overflow:hidden;
}
.progress-bar{
  height: 14px;
  background: #1f77b4;
  width: 0%;
}
.progress-end{
  flex:0 0 170px;
  text-align:right;
  font-variant-numeric: tabular-nums;
  white-space:nowrap;
}

@media (max-width: 900px){
  .progress-label{flex:0 0 140px;}
  .progress-end{flex:0 0 130px;}
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("Controle Financeiro do Casal")

df_gastos, df_metas, df_fixas, df_reservas, df_config = carregar_excel()

# Sidebar
st.sidebar.subheader("Menu")
pagina = st.sidebar.radio(
    "",
    [
        "Lançar",
        "Resumo",
        "Gerenciar",
        "Metas de gastos",
        "Reserva",
        "Contas Fixas",
        "Cadastros",
        "Backup/Restore",
    ],
)

st.sidebar.divider()
st.sidebar.subheader("Período")

anos_disponiveis = sorted(list(set(df_gastos["Data"].dropna().dt.year.astype(int).tolist() + [date.today().year])))
ano_sel = st.sidebar.selectbox("Ano", anos_disponiveis, index=anos_disponiveis.index(date.today().year) if date.today().year in anos_disponiveis else 0)
mes_nome = st.sidebar.selectbox("Mês", [m[0] for m in MESES], index=date.today().month - 1)
mes_sel = dict(MESES)[mes_nome]

ini, fim = periodo_inicio_fim(int(ano_sel), int(mes_sel))

df_periodo = df_gastos[(df_gastos["Data"] >= ini) & (df_gastos["Data"] < fim)].copy()
df_var = df_periodo[df_periodo["Tipo"].fillna("Variável") != "Fixa"].copy()
df_fix_lanc = df_periodo[df_periodo["Tipo"].fillna("") == "Fixa"].copy()

fixas_ativas = df_fixas[df_fixas["Ativa"] == True].copy()
fixas_total_prev = float(pd.to_numeric(fixas_ativas["Valor"], errors="coerce").fillna(0).sum())
fixas_lancadas_total = float(pd.to_numeric(df_fix_lanc["Valor"], errors="coerce").fillna(0).sum())
fixas_restantes = max(fixas_total_prev - fixas_lancadas_total, 0)

gasto_var_mes = float(pd.to_numeric(df_var["Valor"], errors="coerce").fillna(0).sum())
total_previsto_mes = gasto_var_mes + fixas_restantes

# meta geral
meta_geral = 0.0
try:
    cfg = df_config[df_config["Chave"] == "META_GERAL"]
    if not cfg.empty:
        meta_geral = float(cfg.iloc[0]["Valor"] or 0.0)
except Exception:
    meta_geral = 0.0

total_real_mes = float(pd.to_numeric(df_periodo["Valor"], errors="coerce").fillna(0).sum())
pct_meta_geral = (total_real_mes / meta_geral * 100.0) if meta_geral and meta_geral > 0 else None


def render_progress_rows(df_rows, label_col, value_col, total_value, end_text_fn):
    """
    Renderiza linhas com barra azul e texto no final.
    """
    if df_rows is None or df_rows.empty:
        st.info("Sem dados no período.")
        return

    total_value = float(total_value) if total_value else 0.0

    for _, r in df_rows.iterrows():
        label = str(r[label_col])
        val = float(r[value_col]) if pd.notna(r[value_col]) else 0.0
        pct = (val / total_value * 100.0) if total_value > 0 else 0.0
        width = max(min(pct, 100.0), 0.0)

        end_txt = end_text_fn(val, pct)

        st.markdown(
            f"""
<div class="progress-row">
  <div class="progress-label">{label}</div>
  <div class="progress-bar-wrap">
    <div class="progress-bar" style="width:{width:.2f}%;"></div>
  </div>
  <div class="progress-end">{end_txt}</div>
</div>
""",
            unsafe_allow_html=True,
        )


# -----------------------------
# Páginas
# -----------------------------
if pagina == "Resumo":
    st.subheader(f"Resumo: {mes_nome}/{ano_sel}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto lançado no mês (sem fixas)", fmt_money(gasto_var_mes))
    c2.metric("Fixas previstas (restantes)", fmt_money(fixas_restantes))
    c3.metric("Total previsto (mês)", fmt_money(total_previsto_mes))
    c4.metric("% Meta Geral", fmt_pct(pct_meta_geral) if pct_meta_geral is not None else "—")

    st.caption(f"Fixas ativas: {fmt_money(fixas_total_prev)} | Fixas já lançadas (estimado): {fmt_money(fixas_lancadas_total)}")
    st.divider()

    # Por categoria (barra)
    st.subheader("Por categoria (lançado - sem fixas)")
    if df_var.empty:
        st.info("Sem lançamentos variáveis no período.")
    else:
        por_cat = (
            df_var.groupby("Categoria", as_index=False)["Valor"]
            .sum()
            .sort_values("Valor", ascending=False)
        )
        total_cat = por_cat["Valor"].sum()

        render_progress_rows(
            por_cat,
            label_col="Categoria",
            value_col="Valor",
            total_value=total_cat,
            end_text_fn=lambda v, p: f"{fmt_money(v)} | {fmt_pct(p)}",
        )

    st.divider()

    # Por forma de pagamento (barra)
    st.subheader("Por forma de pagamento (lançado - sem fixas)")
    if df_var.empty:
        st.info("Sem lançamentos variáveis no período.")
    else:
        por_pag = (
            df_var.groupby("Pagamento", as_index=False)["Valor"]
            .sum()
            .sort_values("Valor", ascending=False)
        )
        total_pag = por_pag["Valor"].sum()

        render_progress_rows(
            por_pag,
            label_col="Pagamento",
            value_col="Valor",
            total_value=total_pag,
            end_text_fn=lambda v, p: f"{fmt_money(v)} | {fmt_pct(p)}",
        )


elif pagina == "Lançar":
    st.subheader("Lançar gasto variável")

    cats = sorted(list(set(df_metas["Categoria"].dropna().astype(str).tolist() + df_fixas["Categoria"].dropna().astype(str).tolist())))
    if not cats:
        cats = ["Outros"]

    with st.form("form_gasto_var"):
        col1, col2, col3 = st.columns(3)
        with col1:
            data = st.date_input("Data", date.today())
            quem = st.selectbox("Quem pagou", PESSOAS_PADRAO)
        with col2:
            categoria = st.selectbox("Categoria", cats + ["Outros"])
            sub = st.text_input("Subcategoria (opcional)")
        with col3:
            valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
            pagamento = st.selectbox("Forma de Pagamento", PAGAMENTOS_PADRAO)

        obs = st.text_input("Observação")
        salvar = st.form_submit_button("Salvar gasto")

    if salvar:
        novo = {
            "ID_Gasto": novo_id("GAS"),
            "Data": pd.to_datetime(data),
            "Categoria": categoria,
            "Subcategoria": sub,
            "Valor": float(valor),
            "Pagamento": pagamento,
            "Quem": quem,
            "Obs": obs,
            "Tipo": "Variável",
            "ID_Fixa": "",
        }
        df_gastos = pd.concat([df_gastos, pd.DataFrame([novo])], ignore_index=True)
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Gasto salvo.")

    st.divider()
    st.subheader("Lançar conta fixa (valor real)")

    fixas_ativas = df_fixas[df_fixas["Ativa"] == True].copy()
    if fixas_ativas.empty:
        st.info("Não há contas fixas ativas. Cadastre em Cadastros.")
    else:
        # monta label sem ID
        fixas_ativas["__label"] = fixas_ativas.apply(
            lambda r: f"{r['Descricao']} | Dia {int(r['Dia_Venc'])} | Padrão: {fmt_money(r['Valor'])}",
            axis=1,
        )

        # verifica se já existe lançamento da fixa no mês
        ids_lancadas = set(df_fix_lanc["ID_Fixa"].dropna().astype(str).tolist())
        def status_label(row):
            tag = " (já lançada no mês)" if str(row["ID_Fixa"]) in ids_lancadas else ""
            return row["__label"] + tag

        fixas_ativas["__pick"] = fixas_ativas.apply(status_label, axis=1)

        escolha = st.selectbox("Escolha a conta fixa", fixas_ativas["__pick"].tolist())
        fx = fixas_ativas.loc[fixas_ativas["__pick"] == escolha].iloc[0]

        # data padrão no mês (dia venc)
        try:
            dpad = date(int(ano_sel), int(mes_sel), min(max(int(fx["Dia_Venc"]), 1), 28))
        except Exception:
            dpad = date.today()

        with st.form("form_fixa"):
            col1, col2, col3 = st.columns(3)
            with col1:
                data_pg = st.date_input("Data pagamento", dpad)
                valor_real = st.number_input("Valor real (R$)", min_value=0.0, step=1.0, value=float(fx["Valor"]))
            with col2:
                pagamento_real = st.selectbox("Forma de Pagamento", PAGAMENTOS_PADRAO, index=PAGAMENTOS_PADRAO.index(fx["Pagamento"]) if fx["Pagamento"] in PAGAMENTOS_PADRAO else 0)
                quem_real = st.selectbox("Quem pagou", PESSOAS_PADRAO, index=PESSOAS_PADRAO.index(fx["Quem"]) if fx["Quem"] in PESSOAS_PADRAO else 0)
            with col3:
                st.text_input("Categoria", value=str(fx["Categoria"]), disabled=True)
                st.text_input("Descrição", value=str(fx["Descricao"]), disabled=True)

            obs_fx = st.text_input("Observação (opcional)")
            btn = st.form_submit_button("Lançar esta fixa")

        if btn:
            novo_fx = {
                "ID_Gasto": novo_id("GAS"),
                "Data": pd.to_datetime(data_pg),
                "Categoria": str(fx["Categoria"]),
                "Subcategoria": str(fx["Descricao"]),
                "Valor": float(valor_real),
                "Pagamento": pagamento_real,
                "Quem": quem_real,
                "Obs": obs_fx,
                "Tipo": "Fixa",
                "ID_Fixa": str(fx["ID_Fixa"]),
            }
            df_gastos = pd.concat([df_gastos, pd.DataFrame([novo_fx])], ignore_index=True)
            salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
            st.success("Conta fixa lançada.")


elif pagina == "Gerenciar":
    st.subheader("Gerenciar lançamentos (editar / apagar)")

    if df_periodo.empty:
        st.info("Sem lançamentos no período.")
    else:
        # mostra sem IDs
        view = df_periodo.copy()
        cols_drop = [c for c in ["ID_Gasto", "ID_Fixa"] if c in view.columns]
        view = view.drop(columns=cols_drop, errors="ignore")
        view["Data"] = view["Data"].dt.strftime("%d/%m/%Y")
        view["Valor"] = pd.to_numeric(view["Valor"], errors="coerce").fillna(0).apply(fmt_money)

        st.dataframe(view.sort_values("Data", ascending=False), use_container_width=True, hide_index=True)

        st.divider()
        st.write("Editar lançamento")
        ids = df_periodo["ID_Gasto"].dropna().astype(str).tolist()
        id_edit = st.selectbox("Selecione um lançamento", ids)

        row = df_gastos[df_gastos["ID_Gasto"].astype(str) == str(id_edit)].iloc[0]

        with st.form("form_edit"):
            col1, col2, col3 = st.columns(3)
            with col1:
                data_n = st.date_input("Data", row["Data"].date() if pd.notna(row["Data"]) else date.today())
                valor_n = st.number_input("Valor (R$)", min_value=0.0, step=1.0, value=float(row["Valor"]))
            with col2:
                cat_n = st.text_input("Categoria", value=str(row["Categoria"]))
                sub_n = st.text_input("Subcategoria", value=str(row["Subcategoria"]))
            with col3:
                pag_n = st.selectbox("Pagamento", PAGAMENTOS_PADRAO, index=PAGAMENTOS_PADRAO.index(row["Pagamento"]) if row["Pagamento"] in PAGAMENTOS_PADRAO else 0)
                quem_n = st.selectbox("Quem", PESSOAS_PADRAO, index=PESSOAS_PADRAO.index(row["Quem"]) if row["Quem"] in PESSOAS_PADRAO else 0)

            obs_n = st.text_input("Obs", value=str(row["Obs"]))
            salvar_edit = st.form_submit_button("Salvar edição")

        if salvar_edit:
            idx = df_gastos.index[df_gastos["ID_Gasto"].astype(str) == str(id_edit)][0]
            df_gastos.loc[idx, "Data"] = pd.to_datetime(data_n)
            df_gastos.loc[idx, "Valor"] = float(valor_n)
            df_gastos.loc[idx, "Categoria"] = cat_n
            df_gastos.loc[idx, "Subcategoria"] = sub_n
            df_gastos.loc[idx, "Pagamento"] = pag_n
            df_gastos.loc[idx, "Quem"] = quem_n
            df_gastos.loc[idx, "Obs"] = obs_n

            salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
            st.success("Edição salva.")

        st.divider()
        st.write("Apagar lançamento")
        id_apagar = st.selectbox("Selecione para apagar", ids, key="apagar_id")
        if st.button("Apagar", type="primary"):
            df_gastos = df_gastos[df_gastos["ID_Gasto"].astype(str) != str(id_apagar)].copy()
            salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
            st.success("Lançamento apagado.")


elif pagina == "Metas de gastos":
    st.subheader(f"Metas de gastos: {mes_nome}/{ano_sel}")

    if df_metas.empty:
        st.info("Cadastre metas em Cadastros.")
    else:
        metas = df_metas.copy()
        metas["Meta"] = pd.to_numeric(metas["Meta"], errors="coerce").fillna(0)

        gastos_cat = (
            df_var.groupby("Categoria", as_index=False)["Valor"]
            .sum()
            .rename(columns={"Valor": "Gasto"})
        )

        base = metas.merge(gastos_cat, on="Categoria", how="left")
        base["Gasto"] = pd.to_numeric(base["Gasto"], errors="coerce").fillna(0)
        base["Falta"] = (base["Meta"] - base["Gasto"]).clip(lower=0)
        base["Pct"] = base.apply(lambda r: (r["Gasto"] / r["Meta"] * 100.0) if r["Meta"] > 0 else None, axis=1)

        base = base.sort_values("Gasto", ascending=False)

        # linhas com barra: progresso do gasto em relação à meta
        for _, r in base.iterrows():
            cat = str(r["Categoria"])
            gasto = float(r["Gasto"])
            meta = float(r["Meta"])
            pct = (gasto / meta * 100.0) if meta > 0 else 0.0
            width = max(min(pct, 100.0), 0.0)

            end_txt = f"{fmt_money(gasto)} / {fmt_money(meta)} | {fmt_pct((gasto/meta*100.0) if meta>0 else None)}"

            st.markdown(
                f"""
<div class="progress-row">
  <div class="progress-label">{cat}</div>
  <div class="progress-bar-wrap">
    <div class="progress-bar" style="width:{width:.2f}%;"></div>
  </div>
  <div class="progress-end">{end_txt}</div>
</div>
""",
                unsafe_allow_html=True,
            )


elif pagina == "Reserva":
    st.subheader("Reserva")

    reservas_ativas = df_reservas[df_reservas["Ativa"] == True].copy()
    reservas_ativas["Meta"] = pd.to_numeric(reservas_ativas["Meta"], errors="coerce").fillna(0)
    reservas_ativas["Reservado"] = pd.to_numeric(reservas_ativas["Reservado"], errors="coerce").fillna(0)

    total_reservado = float(reservas_ativas["Reservado"].sum())
    meta_total = float(reservas_ativas["Meta"].sum())
    falta = max(meta_total - total_reservado, 0)
    pct = (total_reservado / meta_total * 100.0) if meta_total > 0 else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total reservado", fmt_money(total_reservado))
    c2.metric("Meta total", fmt_money(meta_total))
    c3.metric("Falta", fmt_money(falta))
    c4.metric("% atingido", fmt_pct(pct) if pct is not None else "—")

    st.divider()

    # barras por reserva: Reservado vs Meta
    for _, r in reservas_ativas.sort_values("Reservado", ascending=False).iterrows():
        nome = str(r["Reserva"])
        reservado = float(r["Reservado"])
        meta = float(r["Meta"])
        pct_r = (reservado / meta * 100.0) if meta > 0 else 0.0
        width = max(min(pct_r, 100.0), 0.0)
        end_txt = f"{fmt_money(reservado)} / {fmt_money(meta)} | {fmt_pct((reservado/meta*100.0) if meta>0 else None)}"

        st.markdown(
            f"""
<div class="progress-row">
  <div class="progress-label">{nome}</div>
  <div class="progress-bar-wrap">
    <div class="progress-bar" style="width:{width:.2f}%;"></div>
  </div>
  <div class="progress-end">{end_txt}</div>
</div>
""",
            unsafe_allow_html=True,
        )


elif pagina == "Contas Fixas":
    st.subheader("Contas fixas (visualização)")
    fixas_ativas = df_fixas[df_fixas["Ativa"] == True].copy()

    if fixas_ativas.empty:
        st.info("Não há contas fixas ativas.")
    else:
        view = fixas_ativas.drop(columns=["ID_Fixa"], errors="ignore").copy()
        view["Valor"] = pd.to_numeric(view["Valor"], errors="coerce").fillna(0).apply(fmt_money)
        st.dataframe(view.sort_values(["Dia_Venc", "Descricao"]), use_container_width=True, hide_index=True)

    st.caption("Edição das contas fixas é feita apenas em Cadastros.")


elif pagina == "Cadastros":
    st.subheader("Cadastros (editar tudo aqui)")

    st.divider()
    st.write("Meta geral (limite mensal)")
    meta_nova = st.number_input("Meta geral (R$)", min_value=0.0, step=100.0, value=float(meta_geral))
    if st.button("Salvar meta geral"):
        if df_config[df_config["Chave"] == "META_GERAL"].empty:
            df_config = pd.concat([df_config, pd.DataFrame([{"Chave": "META_GERAL", "Valor": float(meta_nova)}])], ignore_index=True)
        else:
            df_config.loc[df_config["Chave"] == "META_GERAL", "Valor"] = float(meta_nova)
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Meta geral salva.")

    st.divider()
    st.write("Metas de gastos")
    df_metas_edit = df_metas.copy()
    edit_metas = st.data_editor(df_metas_edit, num_rows="dynamic", use_container_width=True, hide_index=True)
    if st.button("Salvar metas de gastos"):
        edit_metas["Meta"] = pd.to_numeric(edit_metas.get("Meta", 0), errors="coerce").fillna(0)
        df_metas = edit_metas.dropna(subset=["Categoria"]).copy()
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Metas salvas.")

    st.divider()
    st.write("Contas fixas")
    df_fixas_edit = df_fixas.copy()
    edit_fixas = st.data_editor(
        df_fixas_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        disabled=["ID_Fixa"],
    )
    if st.button("Salvar contas fixas"):
        edit_fixas["ID_Fixa"] = edit_fixas["ID_Fixa"].fillna("").astype(str)
        fors = edit_fixas
        for i in range(len(Ros := R ors := edit_fixas)):
            if Ros.loc[i, "ID_Fixa"].strip() == "":
                Ros.loc[i, "ID_Fixa"] = novo_id("FIX")

        edit_fixas["Valor"] = pd.to_numeric(edit_fixas.get("Valor", 0), errors="coerce").fillna(0)
        edit_fixas["Dia_Venc"] = pd.to_numeric(edit_fixas.get("Dia_Venc", 1), errors="coerce").fillna(1).astype(int)
        edit_fixas["Ativa"] = edit_fixas.get("Ativa", True).fillna(True).astype(bool)

        df_fixas = edit_fixas.dropna(subset=["Descricao"]).copy()
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Contas fixas salvas.")

    st.divider()
    st.write("Reservas")
    df_res_edit = df_reservas.copy()
    edit_res = st.data_editor(
        df_res_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        disabled=["ID_Reserva"],
    )
    if st.button("Salvar reservas"):
        edit_res["ID_Reserva"] = edit_res["ID_Reserva"].fillna("").astype(str)
        for i in range(len(edit_res)):
            if edit_res.loc[i, "ID_Reserva"].strip() == "":
                edit_res.loc[i, "ID_Reserva"] = novo_id("RES")

        edit_res["Meta"] = pd.to_numeric(edit_res.get("Meta", 0), errors="coerce").fillna(0)
        edit_res["Reservado"] = pd.to_numeric(edit_res.get("Reservado", 0), errors="coerce").fillna(0)
        edit_res["Ativa"] = edit_res.get("Ativa", True).fillna(True).astype(bool)

        df_reservas = edit_res.dropna(subset=["Reserva"]).copy()
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Reservas salvas.")


elif pagina == "Backup/Restore":
    st.subheader("Backup / Restore")

    st.write("Backup do arquivo atual")
    if os.path.exists(ARQUIVO):
        with open(ARQUIVO, "rb") as f:
            st.download_button(
                "Baixar backup (dados.xlsx)",
                data=f,
                file_name=f"dados_backup_{date.today().isoformat()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    st.divider()
    st.write("Restaurar um backup")
    up = st.file_uploader("Envie um .xlsx para substituir o atual", type=["xlsx"])
    if up is not None:
        if st.button("Restaurar agora", type="primary"):
            tmp_dir = tempfile.mkdtemp()
            tmp_file = os.path.join(tmp_dir, "restore.xlsx")
            with open(tmp_file, "wb") as f:
                f.write(up.getbuffer())
            shutil.move(tmp_file, ARQUIVO)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            carregar_excel_cached.clear()
            st.success("Backup restaurado. Recarregue a página.")
