import pandas as pd
import streamlit as st
from datetime import date

ARQUIVO = "dados.xlsx"

df = pd.read_excel(ARQUIVO, sheet_name="gastos")
metas = pd.read_excel(ARQUIVO, sheet_name="metas")

st.set_page_config(page_title="Controle Financeiro", layout="centered")

st.title("💰 Controle Financeiro do Casal")

# carregar dados
df = pd.read_excel(ARQUIVO, sheet_name="gastos")
metas = pd.read_excel(ARQUIVO, sheet_name="metas")

# formulário
with st.form("novo_gasto"):
    data = st.date_input("Data", date.today())
    categoria = st.selectbox(
        "Categoria",
        metas["Categoria"].unique()
    )
    sub = st.text_input("Subcategoria (opcional)")
    valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
    quem = st.selectbox("Quem pagou", ["Roney", "Adriele"])
    Pagamento = st.selectbox("Forma de Pagamento",["PIX","Cartão Pão de Açucar","Cartão Nubank","Swile","Pluxee"])
    obs = st.text_input("Observação")

    salvar = st.form_submit_button("Salvar Gasto")

if salvar:
    novo = {
        "Data": data,
        "Categoria": categoria,
        "Subcategoria": sub,
        "Valor": valor,
        "Pagamento": Pagamento,
        "Quem": quem,
        "Obs": obs
    }
    df = pd.concat([df, pd.DataFrame([novo])])
    df.to_excel(ARQUIVO, sheet_name="gastos", index=False)
    st.success("✅ Gasto salvo com sucesso!")

    df = pd.read_excel(ARQUIVO, sheet_name="gastos")

# garante Data como data para ordenar corretamente
if "Data" in df.columns:
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")

st.subheader("Últimos lançamentos")
st.dataframe(
    df.sort_values("Data", ascending=False).head(50),
    use_container_width=True

# relatório do mês
df["Data"] = pd.to_datetime(df["Data"])
mes_atual = df[df["Data"].dt.month == date.today().month]

total_mes = mes_atual["Valor"].sum()

st.divider()
st.subheader("📊 Resumo do Mês")
st.metric("Total Gasto no Mês", f"R$ {total_mes:,.2f}")

