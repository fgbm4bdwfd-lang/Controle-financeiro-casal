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
# Helpers de formatação
# -----------------------------
def fmt_money(v) -> str:
    try:
        if pd.isna(v):
            return "R$ 0,00"
        v = float(v)
    except Exception:
        return "R$ 0,00"
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def fmt_pct(p) -> str:
    try:
        if pd.isna(p):
            return "—"
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
# Leitura / escrita segura no Excel
# (evita BadZipFile / arquivo truncado)
# -----------------------------
def acquire_lock(lock_path: str, timeout_s: int = 15) -> bool:
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
        st.error("Não consegui obter lock para salvar. Recarregue a página e tente novamente.")
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
            "Tipo",       # "Variável" ou "Fixa"
            "ID_Fixa",    # referência quando Tipo="Fixa"
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

    # Reservas essenciais (padrão)
    df_reservas = pd.DataFrame(
        [
            {"ID_Reserva": novo_id("RES"), "Reserva": "Emergência (6 meses)", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Saúde", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Manutenção carro", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Manutenção casa", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Viagens / Lazer", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Impostos / taxas anuais", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
            {"ID_Reserva": novo_id("RES"), "Reserva": "Oportunidades", "Meta": 0.0, "Reservado": 0.0, "Ativa": True},
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
    try:
        mtime = os.path.getmtime(ARQUIVO)
        df_gastos, df_metas, df_fixas, df_reservas, df_config = carregar_excel_cached(ARQUIVO, mtime)
    except Exception as e:
        st.error("Erro ao ler o Excel (possível arquivo corrompido). Vá em Backup/Restore e restaure um backup.")
        raise e

    # Normalizações
    if "Data" in df_gastos.columns:
        df_gastos["Data"] = pd.to_datetime(df_gastos["Data"], errors="coerce")

    # Tipos
    if "Tipo" not in df_gastos.columns:
        df_gastos["Tipo"] = "Variável"
    if "ID_Gasto" not in df_gastos.columns:
        df_gastos["ID_Gasto"] = [novo_id("GAS") for _ in range(len(df_gastos))]
    if "ID_Fixa" not in df_gastos.columns:
        df_gastos["ID_Fixa"] = ""

    if "Ativa" in df_fixas.columns:
        df_fixas["Ativa"] = df_fixas["Ativa"].fillna(True).astype(bool)
    else:
        df_fixas["Ativa"] = True

    if "ID_Fixa" not in df_fixas.columns:
        df_fixas["ID_Fixa"] = [novo_id("FIX") for _ in range(len(df_fixas))]

    if df_reservas is None or df_reservas.empty:
        df_reservas = pd.DataFrame(columns=["ID_Reserva", "Reserva", "Meta", "Reservado", "Ativa"])
    if "Ativa" in df_reservas.columns:
        df_reservas["Ativa"] = df_reservas["Ativa"].fillna(True).astype(bool)
    else:
        df_reservas["Ativa"] = True
    if "ID_Reserva" not in df_reservas.columns:
        df_reservas["ID_Reserva"] = [novo_id("RES") for _ in range(len(df_reservas))]

    if df_config is None or df_config.empty:
        df_config = pd.DataFrame([{"Chave": "META_GERAL", "Valor": 0.0}])

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
# UI / Layout
# -----------------------------
st.set_page_config(page_title="Controle Financeiro do Casal", layout="wide")

st.markdown(
    """
    <style>
      header, footer {visibility: hidden;}
      h1 {font-size: 2.0rem !important;}
      h2 {font-size: 1.35rem !important;}
      div[data-testid="stMetricValue"] {font-size: 1.55rem !important;}
      div[data-testid="stMetricLabel"] {font-size: 0.90rem !important;}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Controle Financeiro do Casal")

df_gastos, df_metas, df_fixas, df_reservas, df_config = carregar_excel()

# Período (sidebar)
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

anos_disponiveis = sorted(
    list(
        set(
            [int(x) for x in df_gastos["Data"].dropna().dt.year.unique().tolist()]
            + [date.today().year]
        )
    )
)

ano_sel = st.sidebar.selectbox("Ano", anos_disponiveis, index=anos_disponiveis.index(date.today().year) if date.today().year in anos_disponiveis else 0)
mes_nome = st.sidebar.selectbox("Mês", [m[0] for m in MESES], index=date.today().month - 1)
mes_sel = dict(MESES)[mes_nome]

ini, fim = periodo_inicio_fim(int(ano_sel), int(mes_sel))

df_periodo = df_gastos[(df_gastos["Data"] >= ini) & (df_gastos["Data"] < fim)].copy()
df_var = df_periodo[df_periodo["Tipo"].fillna("Variável") != "Fixa"].copy()
df_fix_lanc = df_periodo[df_periodo["Tipo"].fillna("") == "Fixa"].copy()

fixas_ativas = df_fixas[df_fixas["Ativa"] == True].copy()
fixas_total_prev = pd.to_numeric(fixas_ativas.get("Valor", 0), errors="coerce").fillna(0).sum()
fixas_lancadas_total = pd.to_numeric(df_fix_lanc.get("Valor", 0), errors="coerce").fillna(0).sum()
fixas_restantes = max(fixas_total_prev - fixas_lancadas_total, 0)

gasto_var_mes = pd.to_numeric(df_var.get("Valor", 0), errors="coerce").fillna(0).sum()
total_previsto_mes = gasto_var_mes + fixas_restantes

# Meta geral (config)
meta_geral = 0.0
try:
    cfg = df_config[df_config["Chave"] == "META_GERAL"]
    if not cfg.empty:
        meta_geral = float(cfg.iloc[0]["Valor"] or 0.0)
except Exception:
    meta_geral = 0.0

total_real_mes = pd.to_numeric(df_periodo.get("Valor", 0), errors="coerce").fillna(0).sum()
pct_meta_geral = (total_real_mes / meta_geral * 100.0) if meta_geral and meta_geral > 0 else None


# -----------------------------
# PÁGINAS
# -----------------------------
if pagina == "Resumo":
    st.subheader(f"Resumo: {mes_nome}/{ano_sel}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto lançado no mês (sem fixas)", fmt_money(gasto_var_mes))
    c2.metric("Fixas previstas (restantes)", fmt_money(fixas_restantes))
    c3.metric("Total previsto (mês)", fmt_money(total_previsto_mes))
    c4.metric("% Meta Geral", fmt_pct(pct_meta_geral) if pct_meta_geral is not None else "—")

    st.caption(
        f"Fixas ativas do mês: {fmt_money(fixas_total_prev)} | Já lançadas/pagas (estimado): {fmt_money(fixas_lancadas_total)}"
    )

    st.divider()

    st.subheader("Por categoria (lançado - sem fixas)")
    if df_var.empty:
        st.info("Sem lançamentos variáveis no período.")
    else:
        por_cat = (
            df_var.assign(ValorNum=pd.to_numeric(df_var["Valor"], errors="coerce").fillna(0))
            .groupby("Categoria", as_index=False)["ValorNum"]
            .sum()
            .rename(columns={"ValorNum": "Valor"})
            .sort_values("Valor", ascending=False)
        )
        total = por_cat["Valor"].sum()
        por_cat["Percentual"] = por_cat["Valor"].apply(lambda x: (x / total * 100.0) if total else 0.0)

        view = por_cat.copy()
        view["Valor"] = view["Valor"].apply(fmt_money)
        view["Percentual"] = view["Percentual"].apply(fmt_pct)

        st.dataframe(view, use_container_width=True, hide_index=True)

    st.subheader("Por pessoa (lançado - sem fixas)")
    if df_var.empty:
        st.info("Sem lançamentos variáveis no período.")
    else:
        por_pessoa = (
            df_var.assign(ValorNum=pd.to_numeric(df_var["Valor"], errors="coerce").fillna(0))
            .groupby("Quem", as_index=False)["ValorNum"]
            .sum()
            .rename(columns={"ValorNum": "Valor"})
            .sort_values("Valor", ascending=False)
        )
        viewp = por_pessoa.copy()
        viewp["Valor"] = viewp["Valor"].apply(fmt_money)
        st.dataframe(viewp, use_container_width=True, hide_index=True)


elif pagina == "Lançar":
    st.subheader("Lançar gasto")

    # Lista de categorias: metas + fixas + (fallback)
    cats = sorted(
        list(
            set(
                df_metas["Categoria"].dropna().astype(str).tolist()
                + df_fixas["Categoria"].dropna().astype(str).tolist()
            )
        )
    )
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
        st.success("Gasto salvo com sucesso.")

    st.divider()
    st.subheader("Contas fixas do mês (lançar uma por uma)")

    fixas_ativas = df_fixas[df_fixas["Ativa"] == True].copy()
    if fixas_ativas.empty:
        st.info("Não há contas fixas ativas cadastradas.")
    else:
        # já lançadas no mês por ID_Fixa
        lancadas_ids = set(df_fix_lanc["ID_Fixa"].dropna().astype(str).tolist())

        for _, fx in fixas_ativas.iterrows():
            id_fixa = str(fx.get("ID_Fixa", ""))
            desc = str(fx.get("Descricao", ""))
            cat = str(fx.get("Categoria", "Outros"))
            val_padrao = float(pd.to_numeric(fx.get("Valor", 0), errors="coerce") or 0)
            dia_venc = int(pd.to_numeric(fx.get("Dia_Venc", 1), errors="coerce") or 1)
            pag_padrao = str(fx.get("Pagamento", "PIX") or "PIX")
            quem_padrao = str(fx.get("Quem", PESSOAS_PADRAO[0]) or PESSOAS_PADRAO[0])

            # data padrão no mês selecionado
            try:
                data_padrao = date(int(ano_sel), int(mes_sel), min(max(dia_venc, 1), 28))
            except Exception:
                data_padrao = date.today()

            ja = id_fixa in lancadas_ids

            titulo = f"{desc} | Venc: dia {dia_venc} | Padrão: {fmt_money(val_padrao)}"
            if ja:
                titulo += "  (JÁ LANÇADA NO MÊS)"

            with st.expander(titulo, expanded=False):
                c1, c2, c3 = st.columns(3)
                with c1:
                    data_pg = st.date_input(f"Data pagamento ({id_fixa})", data_padrao, key=f"dp_{id_fixa}")
                    valor_real = st.number_input(f"Valor real ({id_fixa})", min_value=0.0, step=1.0, value=float(val_padrao), key=f"vr_{id_fixa}")
                with c2:
                    pagamento_real = st.selectbox(f"Pagamento ({id_fixa})", PAGAMENTOS_PADRAO, index=PAGAMENTOS_PADRAO.index(pag_padrao) if pag_padrao in PAGAMENTOS_PADRAO else 0, key=f"pg_{id_fixa}")
                    quem_real = st.selectbox(f"Quem ({id_fixa})", PESSOAS_PADRAO, index=PESSOAS_PADRAO.index(quem_padrao) if quem_padrao in PESSOAS_PADRAO else 0, key=f"qm_{id_fixa}")
                with c3:
                    obs_fx = st.text_input(f"Obs ({id_fixa})", value="", key=f"ob_{id_fixa}")
                    st.caption(f"Categoria: {cat}")

                colb1, colb2 = st.columns([1, 3])
                with colb1:
                    btn = st.button("Lançar esta fixa", key=f"btn_{id_fixa}")

                if btn:
                    novo_fx = {
                        "ID_Gasto": novo_id("GAS"),
                        "Data": pd.to_datetime(data_pg),
                        "Categoria": cat,
                        "Subcategoria": desc,
                        "Valor": float(valor_real),
                        "Pagamento": pagamento_real,
                        "Quem": quem_real,
                        "Obs": obs_fx,
                        "Tipo": "Fixa",
                        "ID_Fixa": id_fixa,
                    }
                    df_gastos = pd.concat([df_gastos, pd.DataFrame([novo_fx])], ignore_index=True)
                    salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
                    st.success("Conta fixa lançada.")


elif pagina == "Gerenciar":
    st.subheader("Editar / apagar lançamentos")

    if df_periodo.empty:
        st.info("Sem lançamentos no período selecionado.")
    else:
        df_show = df_periodo.copy()
        df_show["Valor"] = pd.to_numeric(df_show["Valor"], errors="coerce").fillna(0)

        # tabela (com moeda)
        view = df_show.copy()
        view["Data"] = view["Data"].dt.strftime("%d/%m/%Y")
        view["Valor"] = view["Valor"].apply(fmt_money)

        st.dataframe(view.sort_values("Data", ascending=False), use_container_width=True, hide_index=True)

        st.divider()
        st.write("Apagar lançamento")
        ids = df_periodo["ID_Gasto"].dropna().astype(str).tolist()
        id_apagar = st.selectbox("Selecione o ID_Gasto para apagar", ids)
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
            df_var.assign(ValorNum=pd.to_numeric(df_var["Valor"], errors="coerce").fillna(0))
            .groupby("Categoria", as_index=False)["ValorNum"]
            .sum()
            .rename(columns={"ValorNum": "Gasto"})
        )

        base = metas.merge(gastos_cat, on="Categoria", how="left")
        base["Gasto"] = base["Gasto"].fillna(0)
        base["Falta"] = (base["Meta"] - base["Gasto"]).clip(lower=0)
        base["% atingido"] = base.apply(lambda r: (r["Gasto"] / r["Meta"] * 100.0) if r["Meta"] > 0 else None, axis=1)

        view = base.copy()
        view["Meta"] = view["Meta"].apply(fmt_money)
        view["Gasto"] = view["Gasto"].apply(fmt_money)
        view["Falta"] = view["Falta"].apply(fmt_money)
        view["% atingido"] = view["% atingido"].apply(fmt_pct)

        st.dataframe(view.sort_values("Categoria"), use_container_width=True, hide_index=True)


elif pagina == "Reserva":
    # fonte menor só aqui
    st.markdown(
        """
        <style>
          div[data-testid="stMetricValue"] {font-size: 1.25rem !important;}
          div[data-testid="stMetricLabel"] {font-size: 0.85rem !important;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.subheader("Reserva")

    if df_reservas.empty:
        st.info("Cadastre reservas em Cadastros.")
    else:
        reservas_ativas = df_reservas[df_reservas["Ativa"] == True].copy()
        reservas_ativas["Meta"] = pd.to_numeric(reservas_ativas["Meta"], errors="coerce").fillna(0)
        reservas_ativas["Reservado"] = pd.to_numeric(reservas_ativas["Reservado"], errors="coerce").fillna(0)

        total_reservado = reservas_ativas["Reservado"].sum()
        meta_total = reservas_ativas["Meta"].sum()
        falta = max(meta_total - total_reservado, 0)
        pct = (total_reservado / meta_total * 100.0) if meta_total > 0 else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total reservado", fmt_money(total_reservado))
        c2.metric("Meta total", fmt_money(meta_total))
        c3.metric("Falta", fmt_money(falta))
        c4.metric("% atingido", fmt_pct(pct) if pct is not None else "—")

        st.divider()

        view = reservas_ativas.copy()
        # ocultar ID_Reserva aqui
        if "ID_Reserva" in view.columns:
            view = view.drop(columns=["ID_Reserva"])
        view["Meta"] = view["Meta"].apply(fmt_money)
        view["Reservado"] = view["Reservado"].apply(fmt_money)

        st.dataframe(view, use_container_width=True, hide_index=True)


elif pagina == "Contas Fixas":
    st.subheader("Contas fixas (visualização)")

    fixas_ativas = df_fixas[df_fixas["Ativa"] == True].copy()
    if fixas_ativas.empty:
        st.info("Não há contas fixas ativas.")
    else:
        view = fixas_ativas.copy()
        # ocultar ID_Fixa aqui
        if "ID_Fixa" in view.columns:
            view = view.drop(columns=["ID_Fixa"])
        view["Valor"] = pd.to_numeric(view["Valor"], errors="coerce").fillna(0).apply(fmt_money)
        st.dataframe(view.sort_values(["Dia_Venc", "Descricao"]), use_container_width=True, hide_index=True)

    st.caption("Edição das contas fixas agora é feita apenas em Cadastros.")


elif pagina == "Cadastros":
    st.subheader("Cadastros (editar tudo aqui)")

    st.divider()
    st.write("Meta geral (limite mensal)")
    meta_nova = st.number_input("Meta geral (R$)", min_value=0.0, step=100.0, value=float(meta_geral))
    if st.button("Salvar meta geral"):
        # garante linha
        if df_config[df_config["Chave"] == "META_GERAL"].empty:
            df_config = pd.concat([df_config, pd.DataFrame([{"Chave": "META_GERAL", "Valor": float(meta_nova)}])], ignore_index=True)
        else:
            df_config.loc[df_config["Chave"] == "META_GERAL", "Valor"] = float(meta_nova)
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Meta geral salva.")

    st.divider()
    st.write("Metas de gastos")
    df_metas_edit = df_metas.copy()
    if df_metas_edit.empty:
        df_metas_edit = pd.DataFrame(columns=["Categoria", "Meta"])
    df_metas_edit["Meta"] = pd.to_numeric(df_metas_edit.get("Meta", 0), errors="coerce").fillna(0)

    edit_metas = st.data_editor(
        df_metas_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
    )
    if st.button("Salvar metas de gastos"):
        edit_metas["Meta"] = pd.to_numeric(edit_metas.get("Meta", 0), errors="coerce").fillna(0)
        df_metas = edit_metas.dropna(subset=["Categoria"]).copy()
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Metas salvas.")

    st.divider()
    st.write("Contas fixas")
    df_fixas_edit = df_fixas.copy()
    if df_fixas_edit.empty:
        df_fixas_edit = pd.DataFrame(columns=["ID_Fixa", "Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativa"])
    if "ID_Fixa" not in df_fixas_edit.columns:
        df_fixas_edit["ID_Fixa"] = [novo_id("FIX") for _ in range(len(df_fixas_edit))]

    edit_fixas = st.data_editor(
        df_fixas_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        disabled=["ID_Fixa"],
    )
    if st.button("Salvar contas fixas"):
        # garante IDs nas linhas novas
        if "ID_Fixa" not in edit_fixas.columns:
            edit_fixas["ID_Fixa"] = ""
        edit_fixas["ID_Fixa"] = edit_fixas["ID_Fixa"].fillna("").astype(str)
        for i in range(len(edit_fixas)):
            if not edit_fixas.loc[i, "ID_Fixa"] or edit_fixas.loc[i, "ID_Fixa"].strip() == "":
                edit_fixas.loc[i, "ID_Fixa"] = novo_id("FIX")

        edit_fixas["Valor"] = pd.to_numeric(edit_fixas.get("Valor", 0), errors="coerce").fillna(0)
        edit_fixas["Dia_Venc"] = pd.to_numeric(edit_fixas.get("Dia_Venc", 1), errors="coerce").fillna(1).astype(int)
        edit_fixas["Ativa"] = edit_fixas.get("Ativa", True).fillna(True).astype(bool)

        df_fixas = edit_fixas.dropna(subset=["Descricao"]).copy()
        salvar_tudo(df_gastos, df_metas, df_fixas, df_reservas, df_config)
        st.success("Contas fixas salvas.")

    st.divider()
    st.write("Reservas")
    df_res_edit = df_reservas.copy()
    if df_res_edit.empty:
        df_res_edit = pd.DataFrame(columns=["ID_Reserva", "Reserva", "Meta", "Reservado", "Ativa"])
    if "ID_Reserva" not in df_res_edit.columns:
        df_res_edit["ID_Reserva"] = [novo_id("RES") for _ in range(len(df_res_edit))]

    edit_res = st.data_editor(
        df_res_edit,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        disabled=["ID_Reserva"],
    )
    if st.button("Salvar reservas"):
        if "ID_Reserva" not in edit_res.columns:
            edit_res["ID_Reserva"] = ""
        edit_res["ID_Reserva"] = edit_res["ID_Reserva"].fillna("").astype(str)
        for i in range(len(edit_res)):
            if not edit_res.loc[i, "ID_Reserva"] or edit_res.loc[i, "ID_Reserva"].strip() == "":
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
            # salva seguro
            tmp_dir = tempfile.mkdtemp()
            tmp_file = os.path.join(tmp_dir, "restore.xlsx")
            with open(tmp_file, "wb") as f:
                f.write(up.getbuffer())
            shutil.move(tmp_file, ARQUIVO)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            carregar_excel_cached.clear()
            st.success("Backup restaurado. Recarregue a página.")
