import os
import io
import uuid
import pandas as pd
import streamlit as st
from datetime import date

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# FUNÇÕES AUXILIARES
# ------------------------------------------------------------
def _normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _to_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def _ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante colunas, tipos e cria ID se não existir.
    """
    df = _normalizar_colunas(df)

    # Colunas esperadas
    cols = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    # ID: cria para linhas sem ID
    def _new_id():
        return uuid.uuid4().hex

    df["ID"] = df["ID"].astype("string")
    faltando = df["ID"].isna() | (df["ID"].str.strip() == "")
    if faltando.any():
        df.loc[faltando, "ID"] = [_new_id() for _ in range(int(faltando.sum()))]

    # Tipos
    df["Data"] = _to_datetime_series(df["Data"]).dt.date
    df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)

    # Strings
    for c in ["Categoria", "Subcategoria", "Pagamento", "Quem", "Obs"]:
        df[c] = df[c].astype("string").fillna("").astype(str)

    # Ordena colunas
    df = df[cols].copy()
    return df

def salvar_dados(arquivo: str, df: pd.DataFrame, metas: pd.DataFrame):
    """
    Regrava as duas abas (gastos e metas) para não perder metas ao salvar.
    """
    df = _ensure_schema(df)

    metas = _normalizar_colunas(metas)
    if "Categoria" not in metas.columns:
        metas["Categoria"] = ""
    if "Meta" not in metas.columns:
        metas["Meta"] = 0

    metas = metas.copy()
    metas["Categoria"] = metas["Categoria"].astype(str).str.strip()
    metas["Meta"] = pd.to_numeric(metas["Meta"], errors="coerce").fillna(0.0)

    with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="gastos", index=False)
        metas.to_excel(writer, sheet_name="metas", index=False)

def carregar_dados(arquivo: str):
    """
    Cria arquivo se não existir. Garante schema e IDs.
    """
    if not os.path.exists(arquivo):
        df_vazio = pd.DataFrame(columns=["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"])
        metas_padrao = pd.DataFrame(
            {
                "Categoria": ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros", "Geral"],
                "Meta": [0, 0, 0, 0, 0, 0],
            }
        )
        salvar_dados(arquivo, df_vazio, metas_padrao)

    df = pd.read_excel(arquivo, sheet_name="gastos")
    metas = pd.read_excel(arquivo, sheet_name="metas")

    df = _ensure_schema(df)
    metas = _normalizar_colunas(metas)

    # Se tiver criado IDs agora, salva de volta para manter consistência
    salvar_dados(arquivo, df, metas)

    return df, metas

def gerar_excel_bytes(df: pd.DataFrame, metas: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="gastos", index=False)
        metas.to_excel(writer, sheet_name="metas", index=False)
    return buffer.getvalue()

def filtro_periodo(df: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    dfx = df.copy()
    dfx["Data"] = pd.to_datetime(dfx["Data"], errors="coerce")
    mask = (dfx["Data"].dt.year == ano) & (dfx["Data"].dt.month == mes)
    dfx = dfx.loc[mask].copy()
    dfx["Data"] = dfx["Data"].dt.date
    return dfx

def soma_segura(s: pd.Series) -> float:
    return float(pd.to_numeric(s, errors="coerce").fillna(0).sum())

# ------------------------------------------------------------
# APP
# ------------------------------------------------------------
df, metas = carregar_dados(ARQUIVO)

st.title("Controle Financeiro do Casal")

with st.sidebar:
    st.header("Filtros")

    # anos disponíveis
    df_tmp = df.copy()
    df_tmp["Data_dt"] = pd.to_datetime(df_tmp["Data"], errors="coerce")
    anos = sorted([int(a) for a in df_tmp["Data_dt"].dt.year.dropna().unique().tolist()])
    hoje = date.today()
    if hoje.year not in anos:
        anos = sorted(list(set(anos + [hoje.year])))

    ano_sel = st.selectbox("Ano", anos, index=anos.index(hoje.year))
    mes_sel = st.selectbox("Mês", [m for m, _ in MESES], index=hoje.month - 1, format_func=lambda m: MES_NOME[m])

    st.divider()
    st.header("Backup")
    st.download_button(
        label="Baixar planilha (Excel) atualizada",
        data=gerar_excel_bytes(df, metas),
        file_name="controle_financeiro_casal.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# Categorias (exclui "Geral" do lançamento)
categorias = (
    metas.get("Categoria", pd.Series([], dtype=str))
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)
categorias_lancamento = [c for c in categorias if c.lower() != "geral"]
if not categorias_lancamento:
    categorias_lancamento = ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros"]

# ------------------------------------------------------------
# NOVO LANÇAMENTO
# ------------------------------------------------------------
st.subheader("Novo gasto")

with st.form("novo_gasto"):
    data_lcto = st.date_input("Data", date.today())
    categoria = st.selectbox("Categoria", categorias_lancamento)
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
    salvar_dados(ARQUIVO, df, metas)
    st.success("Gasto salvo.")
    df, metas = carregar_dados(ARQUIVO)

# ------------------------------------------------------------
# PAINEL DO PERÍODO
# ------------------------------------------------------------
st.divider()
st.subheader(f"Painel do período: {MES_NOME[mes_sel]}/{ano_sel}")

df_periodo = filtro_periodo(df, ano_sel, mes_sel)
total_periodo = soma_segura(df_periodo["Valor"]) if not df_periodo.empty else 0.0
st.metric("Total no período", f"R$ {total_periodo:,.2f}")

# ------------------------------------------------------------
# GERENCIAR LANÇAMENTOS (EDITAR / APAGAR)
# ------------------------------------------------------------
st.divider()
st.subheader("Gerenciar lançamentos (editar / apagar)")

colA, colB = st.columns([1, 1])
with colA:
    editar_todos = st.checkbox("Editar todos os lançamentos (não só o período)", value=False)
with colB:
    mostrar_id = st.checkbox("Mostrar coluna ID", value=False)

df_view = df.copy() if editar_todos else df_periodo.copy()

if df_view.empty:
    st.write("Não há lançamentos para gerenciar neste filtro.")
else:
    # Guarda IDs originais da visão para merge correto ao salvar
    view_key = f"VIEW::{ano_sel}-{mes_sel}::{'ALL' if editar_todos else 'PERIODO'}"
    st.session_state[view_key] = df_view["ID"].tolist()

    # Configuração de colunas no editor
    cols_exibir = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]
    if not mostrar_id:
        cols_exibir = ["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]

    df_editor_base = df_view[["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]].copy()

    st.write("Edite valores diretamente. Para apagar, remova a linha na tabela (e depois clique em 'Salvar alterações').")

    edited = st.data_editor(
        df_editor_base if mostrar_id else df_editor_base.drop(columns=["ID"]),
        num_rows="dynamic",
        use_container_width=True,
        key=f"editor::{view_key}",
    )

    # Ao editar sem mostrar ID, recoloca ID usando a visão original e posição das linhas
    # (Para delete funcionar corretamente, recomendamos deixar 'Mostrar coluna ID' ligado.)
    if not mostrar_id:
        # Reconstroi com IDs por aproximação: assume que linhas restantes correspondem às primeiras N do original
        # Melhor prática: use mostrar_id=True quando for apagar linhas.
        ids_orig = st.session_state[view_key]
        edited = edited.copy()
        edited.insert(0, "ID", ids_orig[: len(edited)])

    # Botões de ação
    c1, c2, c3 = st.columns([1, 1, 2])

    with c1:
        salvar_alteracoes = st.button("Salvar alterações", type="primary")
    with c2:
        recarregar = st.button("Recarregar dados")
    with c3:
        st.caption("Dica: para apagar com segurança, marque 'Mostrar coluna ID', apague a linha e salve.")

    if recarregar:
        df, metas = carregar_dados(ARQUIVO)
        st.rerun()

    if salvar_alteracoes:
        # Normaliza e valida o dataframe editado
        edited = edited.copy()
        edited = _normalizar_colunas(edited)

        # Garante colunas presentes
        for c in ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]:
            if c not in edited.columns:
                edited[c] = pd.NA

        edited["ID"] = edited["ID"].astype("string")
        edited["Data"] = _to_datetime_series(edited["Data"]).dt.date
        edited["Valor"] = pd.to_numeric(edited["Valor"], errors="coerce").fillna(0.0)
        for c in ["Categoria", "Subcategoria", "Pagamento", "Quem", "Obs"]:
            edited[c] = edited[c].astype("string").fillna("").astype(str)

        # Merge: remove da base os IDs da visão original e coloca os editados no lugar
        ids_para_substituir = set(st.session_state[view_key])
        df_outros = df.loc[~df["ID"].isin(ids_para_substituir)].copy()

        df_novo = pd.concat([df_outros, edited[["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]]], ignore_index=True)
        df_novo = _ensure_schema(df_novo)

        salvar_dados(ARQUIVO, df_novo, metas)
        st.success("Alterações salvas.")
        df, metas = carregar_dados(ARQUIVO)
        st.rerun()

    # Exclusão rápida (opcional)
    st.divider()
    st.subheader("Excluir lançamento rápido (opcional)")

    # Cria rótulos para seleção
    df_sel = df_view.copy()
    df_sel["Data"] = pd.to_datetime(df_sel["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
    df_sel["Rotulo"] = df_sel.apply(
        lambda r: f"{r['Data']} | {r['Categoria']} | R$ {float(r['Valor']):,.2f} | {r['Quem']} | {r['Pagamento']} | ID={r['ID']}",
        axis=1
    )

    escolha = st.selectbox("Selecione um lançamento para excluir", df_sel["Rotulo"].tolist())
    confirmar = st.checkbox("Confirmo a exclusão definitiva deste lançamento", value=False)

    if st.button("Excluir selecionado") and confirmar:
        id_escolhido = escolha.split("ID=")[-1].strip()
        df = df.loc[df["ID"] != id_escolhido].copy()
        salvar_dados(ARQUIVO, df, metas)
        st.success("Lançamento excluído.")
        df, metas = carregar_dados(ARQUIVO)
        st.rerun()

# ------------------------------------------------------------
# RESUMOS
# ------------------------------------------------------------
st.divider()
st.subheader("Resumo do período")

if df_periodo.empty:
    st.write("Sem lançamentos neste período.")
else:
    resumo_cat = (
        df_periodo.groupby("Categoria")["Valor"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    st.write("Por categoria")
    st.dataframe(resumo_cat, use_container_width=True)

    resumo_pessoa = (
        df_periodo.groupby("Quem")["Valor"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    st.write("Por pessoa")
    st.dataframe(resumo_pessoa, use_container_width=True)

    resumo_pag = (
        df_periodo.groupby("Pagamento")["Valor"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    st.write("Por forma de pagamento")
    st.dataframe(resumo_pag, use_container_width=True)
