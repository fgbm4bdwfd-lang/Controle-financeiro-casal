import os
import io
import pandas as pd
import streamlit as st
from datetime import date
from openpyxl import load_workbook

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
st.set_page_config(page_title="Controle Financeiro", layout="centered")
ARQUIVO = "dados.xlsx"

PAGAMENTOS = ["PIX", "Cartão Pão de Açucar", "Cartão Nubank", "Swile", "Pluxee"]
PESSOAS = ["Roney", "Adriele"]

# ------------------------------------------------------------
# FUNÇÕES
# ------------------------------------------------------------
def _normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def carregar_dados(arquivo: str):
    """
    Carrega abas 'gastos' e 'metas'. Se não existir, cria estrutura mínima.
    """
    if not os.path.exists(arquivo):
        df_vazio = pd.DataFrame(columns=["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"])
        metas_padrao = pd.DataFrame(
            {
                "Categoria": ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros", "Geral"],
                "Meta": [0, 0, 0, 0, 0, 0],
            }
        )
        salvar_dados(arquivo, df_vazio, metas_padrao)
        return df_vazio, metas_padrao

    # lê abas
    df = pd.read_excel(arquivo, sheet_name="gastos")
    metas = pd.read_excel(arquivo, sheet_name="metas")

    df = _normalizar_colunas(df)
    metas = _normalizar_colunas(metas)

    # sanitiza colunas esperadas
    for col in ["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]:
        if col not in df.columns:
            df[col] = pd.NA

    if "Categoria" not in metas.columns:
        raise KeyError("A aba 'metas' precisa ter a coluna 'Categoria'.")
    if "Meta" not in metas.columns:
        metas["Meta"] = 0

    metas["Categoria"] = metas["Categoria"].astype(str).str.strip()

    return df, metas

def salvar_dados(arquivo: str, df: pd.DataFrame, metas: pd.DataFrame):
    """
    Salva SEM apagar a aba metas: regrava o arquivo inteiro com as duas abas.
    """
    # garante ordem de colunas
    cols = ["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[cols].copy()

    with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="gastos", index=False)
        metas.to_excel(writer, sheet_name="metas", index=False)

# ------------------------------------------------------------
# APP
# ------------------------------------------------------------
try:
    df, metas = carregar_dados(ARQUIVO)
except Exception as e:
    st.error(f"Erro ao carregar o arquivo '{ARQUIVO}': {e}")
    st.stop()

st.title("Controle Financeiro do Casal")

# Lista de categorias (exclui "Geral" do lançamento)
categorias = (
    metas["Categoria"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)
categorias_lancamento = [c for c in categorias if c.lower() != "geral"]

if len(categorias_lancamento) == 0:
    st.error("A aba 'metas' precisa ter pelo menos uma categoria (além de 'Geral').")
    st.stop()

# --------------------------
# FORMULÁRIO
# --------------------------
with st.form("novo_gasto"):
    data_lcto = st.date_input("Data", date.today())
    categoria = st.selectbox("Categoria", categorias_lancamento)
    sub = st.text_input("Subcategoria (opcional)")
    valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
    quem = st.selectbox("Quem pagou", PESSOAS)
    pagamento = st.selectbox("Forma de Pagamento", PAGAMENTOS)
    obs = st.text_input("Observação")

    salvar = st.form_submit_button("Salvar Gasto")

if salvar:
    novo = {
        "Data": data_lcto,
        "Categoria": categoria,
        "Subcategoria": sub,
        "Valor": float(valor),
        "Pagamento": pagamento,
        "Quem": quem,
        "Obs": obs,
    }

    # garante colunas e adiciona linha
    for k in novo.keys():
        if k not in df.columns:
            df[k] = pd.NA

    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)

    try:
        salvar_dados(ARQUIVO, df, metas)
        st.success("Gasto salvo com sucesso.")
        # recarrega para refletir imediatamente
        df, metas = carregar_dados(ARQUIVO)
    except Exception as e:
        st.error(f"Não foi possível salvar no arquivo '{ARQUIVO}': {e}")

# --------------------------
# EXTRATO (ÚLTIMOS LANÇAMENTOS)
# --------------------------
# converte Data para datetime para ordenar e filtrar
df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

st.subheader("Últimos lançamentos")
df_extrato = df.sort_values("Data", ascending=False).head(50).copy()
st.dataframe(df_extrato, use_container_width=True)

# --------------------------
# RESUMO DO MÊS
# --------------------------
hoje = pd.Timestamp(date.today())
mes_atual_mask = (df["Data"].dt.year == hoje.year) & (df["Data"].dt.month == hoje.month)
mes_atual = df.loc[mes_atual_mask].copy()

total_mes = float(mes_atual["Valor"].fillna(0).sum())

st.divider()
st.subheader("Resumo do mês")
st.metric("Total gasto no mês", f"R$ {total_mes:,.2f}")

# Resumo por categoria
if not mes_atual.empty:
    resumo_cat = (
        mes_atual.groupby("Categoria")["Valor"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
else:
    resumo_cat = pd.DataFrame(columns=["Categoria", "Valor"])

st.subheader("Gastos por categoria (mês)")
st.dataframe(resumo_cat, use_container_width=True)

# Meta geral (se existir)
meta_geral = metas.loc[metas["Categoria"].str.lower() == "geral", "Meta"]
if len(meta_geral) > 0:
    mg = float(pd.to_numeric(meta_geral.iloc[0], errors="coerce") or 0)
    if mg > 0:
        progresso = min(total_mes / mg, 1.0)
        st.subheader("Meta geral (mês)")
        st.progress(progresso)
        st.write(f"R$ {total_mes:,.2f} / R$ {mg:,.2f}")
        if total_mes > mg:
            st.warning("Meta geral ultrapassada.")
