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

PAGAMENTOS = ["PIX", "Boleto", "Cartão Pão de Açucar", "Cartão Nubank", "Swile", "Pluxee"]
PESSOAS = ["Roney", "Adriele"]

# -----------------------------
# CARTÕES / REGRAS ANTI-DUPLICIDADE
# -----------------------------
CARTOES = ["Cartão Pão de Açucar", "Cartão Nubank"]

# Se Origem for uma destas, NÃO entra nos totais (evita duplicar compra no cartão + pagamento da fatura)
ORIGENS_NAO_CONTABILIZAR = ["PAGTO_FATURA"]

MESES = [
    (1, "Janeiro"), (2, "Fevereiro"), (3, "Março"), (4, "Abril"),
    (5, "Maio"), (6, "Junho"), (7, "Julho"), (8, "Agosto"),
    (9, "Setembro"), (10, "Outubro"), (11, "Novembro"), (12, "Dezembro"),
]
MES_NOME = {n: nome for n, nome in MESES}

# -----------------------------
# FORMATADORES (MOEDA / %)
# -----------------------------
def fmt_brl(v) -> str:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    s = f"{x:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def fmt_pct(v) -> str:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    return f"{x:.2f}%"

# -----------------------------
# COLUNAS / SCHEMAS
# -----------------------------
GASTOS_COLS = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs", "Origem", "RefFixa"]
METAS_COLS = ["Categoria", "Meta"]
FIXAS_COLS = ["ID_Fixa", "Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]

RESERVAS_COLS = ["ID_Reserva", "Reserva", "Meta", "Ativo", "Obs"]
MOVRES_COLS = ["ID_Mov", "Data", "ID_Reserva", "Reserva", "Tipo", "Valor", "Quem", "Obs"]

# -----------------------------
# LOCK (evita corrupção do xlsx)
# -----------------------------
def acquire_lock(lock_path: str, stale_seconds: int = 180, timeout_seconds: int = 6) -> bool:
    start = time.time()
    while True:
        try:
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
# SCHEMAS
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

    if not (metas["Categoria"].str.lower() == "geral").any():
        metas = pd.concat([metas, pd.DataFrame([{"Categoria": "Geral", "Meta": 0}])], ignore_index=True)
        changed = True

    metas = metas[metas["Categoria"].astype(str).str.strip() != ""].copy()
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

    fixas["Descricao"] = fixas["Descricao"].astype("string").fillna("").astype(str).str.strip()
    fixas["Categoria"] = fixas["Categoria"].astype("string").fillna("").astype(str).str.strip()
    fixas["Valor"] = pd.to_numeric(fixas["Valor"], errors="coerce").fillna(0.0)
    fixas["Dia_Venc"] = pd.to_numeric(fixas["Dia_Venc"], errors="coerce").fillna(1).astype(int)
    fixas["Pagamento"] = fixas["Pagamento"].astype("string").fillna("").astype(str)
    fixas["Quem"] = fixas["Quem"].astype("string").fillna("").astype(str)

    if fixas["Ativo"].dtype != bool:
        fixas["Ativo"] = fixas["Ativo"].astype(str).str.strip().str.lower().isin(["true", "1", "sim", "yes", "y"])
        changed = True

    fixas["Obs"] = fixas["Obs"].astype("string").fillna("").astype(str)

    fixas = fixas[fixas["Descricao"].astype(str).str.strip() != ""].copy()
    return fixas[FIXAS_COLS].copy(), changed

def _ensure_reservas_schema(res: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    changed = False
    res = _normalizar_colunas(res)

    for c in RESERVAS_COLS:
        if c not in res.columns:
            res[c] = pd.NA
            changed = True

    res["ID_Reserva"] = res["ID_Reserva"].astype("string").fillna("").astype(str)
    faltando = res["ID_Reserva"].str.strip() == ""
    if faltando.any():
        res.loc[faltando, "ID_Reserva"] = [uuid.uuid4().hex for _ in range(int(faltando.sum()))]
        changed = True

    res["Reserva"] = res["Reserva"].astype("string").fillna("").astype(str).str.strip()
    res["Meta"] = pd.to_numeric(res["Meta"], errors="coerce").fillna(0.0)

    if res["Ativo"].dtype != bool:
        res["Ativo"] = res["Ativo"].astype(str).str.strip().str.lower().isin(["true", "1", "sim", "yes", "y"])
        changed = True

    res["Obs"] = res["Obs"].astype("string").fillna("").astype(str)

    res = res[res["Reserva"].astype(str).str.strip() != ""].copy()
    return res[RESERVAS_COLS].copy(), changed

def _ensure_movres_schema(mov: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    changed = False
    mov = _normalizar_colunas(mov)

    for c in MOVRES_COLS:
        if c not in mov.columns:
            mov[c] = pd.NA
            changed = True

    mov["ID_Mov"] = mov["ID_Mov"].astype("string").fillna("").astype(str)
    faltando = mov["ID_Mov"].str.strip() == ""
    if faltando.any():
        mov.loc[faltando, "ID_Mov"] = [uuid.uuid4().hex for _ in range(int(faltando.sum()))]
        changed = True

    mov["Data"] = pd.to_datetime(mov["Data"], errors="coerce").dt.date
    mov["ID_Reserva"] = mov["ID_Reserva"].astype("string").fillna("").astype(str)
    mov["Reserva"] = mov["Reserva"].astype("string").fillna("").astype(str).str.strip()
    mov["Tipo"] = mov["Tipo"].astype("string").fillna("Aporte").astype(str).str.strip()
    mov["Valor"] = pd.to_numeric(mov["Valor"], errors="coerce").fillna(0.0)
    mov["Quem"] = mov["Quem"].astype("string").fillna("").astype(str)
    mov["Obs"] = mov["Obs"].astype("string").fillna("").astype(str)

    return mov[MOVRES_COLS].copy(), changed

def _default_frames():
    g = pd.DataFrame(columns=GASTOS_COLS)
    m = pd.DataFrame(
        {"Categoria": ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros", "Geral"],
         "Meta": [0, 0, 0, 0, 0, 0]}
    )
    f = pd.DataFrame(columns=FIXAS_COLS)

    reservas_base = [
        ("Reserva de Emergência", 0, True, "Ideal: 3 a 6 meses do custo de vida."),
        ("Reserva Saúde", 0, True, "Remédios, consultas, exames."),
        ("Reserva Casa", 0, True, "Manutenção, móveis, imprevistos."),
        ("Reserva Veículo/Transporte", 0, True, "IPVA, pneus, revisões."),
        ("Reserva Impostos/Taxas", 0, True, "Taxas anuais e impostos."),
        ("Reserva Viagens/Lazer", 0, True, "Planejamento de viagens."),
        ("Reserva Educação", 0, True, "Cursos, livros, certificações."),
        ("Reserva Presentes/Família", 0, True, "Datas comemorativas."),
    ]
    r = pd.DataFrame([{
        "ID_Reserva": uuid.uuid4().hex,
        "Reserva": nome,
        "Meta": meta,
        "Ativo": ativo,
        "Obs": obs
    } for nome, meta, ativo, obs in reservas_base], columns=RESERVAS_COLS)

    mov = pd.DataFrame(columns=MOVRES_COLS)

    g, _ = _ensure_gastos_schema(g)
    m, _ = _ensure_metas_schema(m)
    f, _ = _ensure_fixas_schema(f)
    r, _ = _ensure_reservas_schema(r)
    mov, _ = _ensure_movres_schema(mov)

    return g, m, f, r, mov

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

def excel_bytes(g, m, f, r, mov) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        g.to_excel(writer, sheet_name="gastos", index=False)
        m.to_excel(writer, sheet_name="metas", index=False)
        f.to_excel(writer, sheet_name="fixas", index=False)
        r.to_excel(writer, sheet_name="reservas", index=False)
        mov.to_excel(writer, sheet_name="mov_reservas", index=False)
    return buffer.getvalue()

def salvar_excel(g, m, f, r, mov, arquivo: str = ARQUIVO):
    ok = acquire_lock(LOCKFILE)
    if not ok:
        raise RuntimeError("Não foi possível salvar agora. Tente novamente.")

    try:
        g2, _ = _ensure_gastos_schema(g)
        m2, _ = _ensure_metas_schema(m)
        f2, _ = _ensure_fixas_schema(f)
        r2, _ = _ensure_reservas_schema(r)
        mov2, _ = _ensure_movres_schema(mov)

        data = excel_bytes(g2, m2, f2, r2, mov2)
        _write_bytes_atomic(arquivo, data)
    finally:
        release_lock(LOCKFILE)

def init_arquivo_se_faltar():
    if os.path.exists(ARQUIVO):
        return
    g, m, f, r, mov = _default_frames()
    salvar_excel(g, m, f, r, mov, ARQUIVO)

@st.cache_data(show_spinner=False)
def carregar_excel_cached(path: str, mtime: float):
    try:
        xls = pd.ExcelFile(path)
        sheets = set(xls.sheet_names)

        gastos = pd.read_excel(xls, sheet_name="gastos") if "gastos" in sheets else pd.DataFrame(columns=GASTOS_COLS)
        metas = pd.read_excel(xls, sheet_name="metas") if "metas" in sheets else pd.DataFrame(columns=METAS_COLS)
        fixas = pd.read_excel(xls, sheet_name="fixas") if "fixas" in sheets else pd.DataFrame(columns=FIXAS_COLS)
        reservas = pd.read_excel(xls, sheet_name="reservas") if "reservas" in sheets else pd.DataFrame(columns=RESERVAS_COLS)
        mov_res = pd.read_excel(xls, sheet_name="mov_reservas") if "mov_reservas" in sheets else pd.DataFrame(columns=MOVRES_COLS)

        return {"ok": True, "gastos": gastos, "metas": metas, "fixas": fixas, "reservas": reservas, "mov_res": mov_res, "error": ""}
    except Exception as e:
        return {"ok": False, "gastos": None, "metas": None, "fixas": None, "reservas": None, "mov_res": None, "error": repr(e)}

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
        g, m, f, r, mov = _default_frames()
        salvar_excel(g, m, f, r, mov, ARQUIVO)
        st.cache_data.clear()
        st.session_state["RECOVERY_MSG"] = (
            "O arquivo dados.xlsx estava corrompido e foi substituído por um novo. "
            f"O antigo foi movido para: {corrompido}. "
            "Vá em Backup/Restore para restaurar um backup."
        )
        return g, m, f, r, mov

    g, chg_g = _ensure_gastos_schema(res["gastos"])
    m, chg_m = _ensure_metas_schema(res["metas"])
    f, chg_f = _ensure_fixas_schema(res["fixas"])
    r, chg_r = _ensure_reservas_schema(res["reservas"])
    mov, chg_mov = _ensure_movres_schema(res["mov_res"])

    if chg_g or chg_m or chg_f or chg_r or chg_mov:
        salvar_excel(g, m, f, r, mov, ARQUIVO)
        st.cache_data.clear()
        return g, m, f, r, mov

    return g, m, f, r, mov

def restore_from_upload(uploaded_file):
    xls = pd.ExcelFile(uploaded_file)
    sheets = set(xls.sheet_names)

    g = pd.read_excel(xls, sheet_name="gastos") if "gastos" in sheets else pd.DataFrame(columns=GASTOS_COLS)
    m = pd.read_excel(xls, sheet_name="metas") if "metas" in sheets else pd.DataFrame(columns=METAS_COLS)
    f = pd.read_excel(xls, sheet_name="fixas") if "fixas" in sheets else pd.DataFrame(columns=FIXAS_COLS)
    r = pd.read_excel(xls, sheet_name="reservas") if "reservas" in sheets else pd.DataFrame(columns=RESERVAS_COLS)
    mov = pd.read_excel(xls, sheet_name="mov_reservas") if "mov_reservas" in sheets else pd.DataFrame(columns=MOVRES_COLS)

    g, _ = _ensure_gastos_schema(g)
    m, _ = _ensure_metas_schema(m)
    f, _ = _ensure_fixas_schema(f)
    r, _ = _ensure_reservas_schema(r)
    mov, _ = _ensure_movres_schema(mov)

    salvar_excel(g, m, f, r, mov, ARQUIVO)
    st.cache_data.clear()
    return g, m, f, r, mov

# -----------------------------
# HELPERS (regras fixas / período / metas)
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
    
def add_meses(d: date, meses: int) -> date:
    """Soma 'meses' meses numa date, preservando o dia quando possível."""
    ano = d.year + (d.month - 1 + meses) // 12
    mes = (d.month - 1 + meses) % 12 + 1
    dia = min(d.day, ultimo_dia_mes(ano, mes))
    return date(ano, mes, dia)

def repartir_valor_em_parcelas(valor_total: float, n: int) -> list[float]:
    """
    Divide valor_total em n parcelas com arredondamento em centavos.
    A última parcela recebe o ajuste para fechar exatamente o total.
    """
    valor_total = float(valor_total)
    n = int(max(1, n))
    base = round(valor_total / n, 2)
    parcelas = [base] * n
    ajuste = round(valor_total - round(base * n, 2), 2)
    parcelas[-1] = round(parcelas[-1] + ajuste, 2)
    return parcelas

def gerar_lancamentos_parcelados(
    df_gastos: pd.DataFrame,
    base: dict,
    parcelas: int,
    primeira_data: date,
    dia_parcela: int | None = None,
):
    """
    Cria N lançamentos (projeção) mês a mês.
    Regra do 'sem dia 1º':
      - se dia_parcela for 1, vira 2
      - se ultrapassar último dia do mês, ajusta para o último dia
    """
    n = int(parcelas)
    if n <= 1:
        return df_gastos

    # dia padrão: usa o dia da primeira_data, com regra de "não dia 1"
    if dia_parcela is None:
        dia_parcela = int(primeira_data.day)
    dia_parcela = max(1, min(int(dia_parcela), 31))
    if dia_parcela == 1:
        dia_parcela = 2

    # ID de grupo (para reconhecer que é a mesma compra parcelada)
    grupo = uuid.uuid4().hex[:10]

    valores = repartir_valor_em_parcelas(float(base["Valor"]), n)

    novos = []
    for i in range(n):
        dt = add_meses(primeira_data, i)
        # aplica dia fixo escolhido
        dt = date(dt.year, dt.month, min(dia_parcela, ultimo_dia_mes(dt.year, dt.month)))

        sub = str(base.get("Subcategoria", "")).strip()
        sufixo = f" (Parcela {i+1}/{n})"
        obs0 = str(base.get("Obs", "")).strip()

        novo = dict(base)
        novo["ID"] = uuid.uuid4().hex
        novo["Data"] = dt
        novo["Valor"] = float(valores[i])
        novo["Origem"] = "PARCELADO"
        novo["Obs"] = (obs0 + (f" | GRUPO={grupo}" if obs0 else f"GRUPO={grupo}")).strip()
        novo["Subcategoria"] = (sub + sufixo).strip() if sub else sufixo.strip()

        novos.append(novo)

    df_gastos = pd.concat([df_gastos, pd.DataFrame(novos)], ignore_index=True)
    df_gastos, _ = _ensure_gastos_schema(df_gastos)
    return df_gastos

def get_meta_geral(df_metas: pd.DataFrame) -> float:
    s = df_metas[df_metas["Categoria"].astype(str).str.strip().str.lower() == "geral"]["Meta"]
    if len(s) == 0:
        return 0.0
    try:
        return float(s.iloc[0])
    except Exception:
        return 0.0

def set_meta_geral(df_metas: pd.DataFrame, novo_valor: float) -> pd.DataFrame:
    m = df_metas.copy()
    mask = m["Categoria"].astype(str).str.strip().str.lower() == "geral"
    if mask.any():
        m.loc[mask, "Meta"] = float(novo_valor)
    else:
        m = pd.concat([m, pd.DataFrame([{"Categoria": "Geral", "Meta": float(novo_valor)}])], ignore_index=True)
    m, _ = _ensure_metas_schema(m)
    return m

def fixas_ativas(df_fixas: pd.DataFrame) -> pd.DataFrame:
    f = df_fixas.copy()
    if f.empty:
        f, _ = _ensure_fixas_schema(f)
    return f[f["Ativo"] == True].copy()

def marcar_e_separar_fixas(df_periodo: pd.DataFrame, df_fixas: pd.DataFrame):
    """
    Regra CORRETA: gasto é FIXA somente se:
      - Origem == 'FIXA' OU
      - RefFixa em IDs das fixas ativas
    (Não usa categoria/subcategoria para evitar classificar variável como fixa.)
    """
    f = fixas_ativas(df_fixas)
    ids_fixas = set(f["ID_Fixa"].astype(str).str.strip().tolist())

    g = df_periodo.copy()
    g["Origem"] = g["Origem"].astype(str)
    g["RefFixa"] = g["RefFixa"].astype(str)

    is_fixa = (g["Origem"].str.upper() == "FIXA") | (g["RefFixa"].str.strip().isin(ids_fixas))

    df_fix = g.loc[is_fixa].copy()
    df_var = g.loc[~is_fixa].copy()
    df_var, _ = _ensure_gastos_schema(df_var)
    df_fix, _ = _ensure_gastos_schema(df_fix)

    total_fixas_planejado = float(f["Valor"].sum()) if not f.empty else 0.0
    total_fixas_lancado = float(df_fix["Valor"].sum()) if not df_fix.empty else 0.0

    return total_fixas_planejado, total_fixas_lancado, df_var, df_fix, f

def ja_lancou_fixa_no_mes(df_gastos: pd.DataFrame, ano: int, mes: int, id_fixa: str) -> bool:
    g = df_gastos.copy()
    g["Data_dt"] = pd.to_datetime(g["Data"], errors="coerce")
    mask_mes = (g["Data_dt"].dt.year == ano) & (g["Data_dt"].dt.month == mes)
    g = g.loc[mask_mes].copy()
    g["Origem"] = g["Origem"].astype(str)
    g["RefFixa"] = g["RefFixa"].astype(str)
    return ((g["Origem"].str.upper() == "FIXA") & (g["RefFixa"].str.strip() == str(id_fixa))).any()

def calcular_saldos_reservas(df_reservas: pd.DataFrame, df_mov: pd.DataFrame) -> pd.DataFrame:
    r = df_reservas.copy()
    mov = df_mov.copy()

    if r.empty:
        r, _ = _ensure_reservas_schema(r)
        r["Saldo"] = 0.0
        r["Percentual"] = 0.0
        r["Falta"] = r["Meta"].astype(float)
        return r

    if mov.empty:
        r["Saldo"] = 0.0
    else:
        mov["Tipo"] = mov["Tipo"].astype(str).str.strip().str.lower()
        mov["Sinal"] = mov["Tipo"].map(lambda x: 1 if "aporte" in x else -1)
        mov["SaldoMov"] = mov["Valor"].astype(float) * mov["Sinal"].astype(float)
        saldo = mov.groupby("ID_Reserva")["SaldoMov"].sum().reset_index()
        saldo.columns = ["ID_Reserva", "Saldo"]
        r = r.merge(saldo, on="ID_Reserva", how="left")
        r["Saldo"] = r["Saldo"].fillna(0.0)

    r["Meta"] = pd.to_numeric(r["Meta"], errors="coerce").fillna(0.0)
    r["Percentual"] = r.apply(lambda x: (x["Saldo"] / x["Meta"] * 100.0) if x["Meta"] > 0 else 0.0, axis=1)
    r["Falta"] = (r["Meta"] - r["Saldo"]).clip(lower=0.0)
    return r

# -----------------------------
# UI: barras por linha (valor + % no final)
# -----------------------------
def render_barras_linhas(itens: list[dict], titulo: str, total_base: float):
    """
    itens: [{"label": "...", "valor": 123.0}]
    total_base: usado para % (se 0, assume soma)
    """
    st.subheader(titulo)

    if not itens:
        st.info("Sem dados no período.")
        return

    soma = sum([float(x.get("valor", 0.0)) for x in itens])
    base = float(total_base) if total_base and total_base > 0 else float(soma)

    css = """
    <style>
    .linha-wrap {margin: 8px 0;}
    .linha-top {display:flex; justify-content:space-between; gap:12px; font-size: 0.95rem;}
    .linha-label {font-weight:600; color:#1f2937;}
    .linha-val {font-variant-numeric: tabular-nums; color:#111827;}
    .barra-bg {width:100%; height: 10px; border-radius: 999px; background: rgba(59,130,246,0.15); overflow:hidden; margin-top: 6px;}
    .barra-fill {height:100%; border-radius: 999px; background: rgba(59,130,246,0.85);}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

    html = ""
    for x in itens:
        label = str(x.get("label", "")).strip()
        valor = float(x.get("valor", 0.0))
        pct = (valor / base * 100.0) if base > 0 else 0.0
        w = max(0.0, min(pct, 100.0))
        html += f"""
        <div class="linha-wrap">
          <div class="linha-top">
            <div class="linha-label">{label}</div>
            <div class="linha-val">{fmt_brl(valor)} | {fmt_pct(pct)}</div>
          </div>
          <div class="barra-bg"><div class="barra-fill" style="width:{w:.2f}%;"></div></div>
        </div>
        """

    st.markdown(html, unsafe_allow_html=True)

def css_metric_compacto():
    st.markdown(
        """
        <style>
        div[data-testid="stMetricValue"] { font-size: 1.55rem !important; }
        div[data-testid="stMetricLabel"] { font-size: 0.85rem !important; }
        </style>
        """,
        unsafe_allow_html=True
    )

# -----------------------------
# LOAD
# -----------------------------
df_gastos, df_metas, df_fixas, df_reservas, df_mov_res = carregar_excel()

if "RECOVERY_MSG" in st.session_state:
    st.warning(st.session_state["RECOVERY_MSG"])

cats = df_metas["Categoria"].dropna().astype(str).str.strip().tolist()
cats_lanc = [c for c in cats if c.lower() != "geral" and c != ""]
if not cats_lanc:
    cats_lanc = ["Alimentação", "Transporte", "Moradia", "Lazer", "Outros"]

hoje = date.today()
tmp = df_gastos.copy()
tmp["Data_dt"] = pd.to_datetime(tmp["Data"], errors="coerce")
anos = sorted([int(a) for a in tmp["Data_dt"].dt.year.dropna().unique().tolist()])
if hoje.year not in anos:
    anos = sorted(list(set(anos + [hoje.year])))

# -----------------------------
# SIDEBAR
# -----------------------------
with st.sidebar:
    menu = st.radio(
        "Menu",
        ["Lançar", "Resumo", "Gerenciar", "Metas de gastos", "Reserva", "Contas Fixas", "Cadastros", "Backup/Restore"],
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
    css_metric_compacto()

    tab_var, tab_fixas = st.tabs(["Gasto variável", "Contas fixas do mês"])

    with tab_var:
        st.subheader("Novo gasto (variável)")

        with st.form("novo_gasto"):
            data_lcto = st.date_input("Data", date.today())
            categoria = st.selectbox("Categoria", cats_lanc)
            sub = st.text_input("Subcategoria (opcional)")
            valor = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
            quem = st.selectbox("Quem pagou", PESSOAS)
            pagamento = st.selectbox("Forma de pagamento", PAGAMENTOS)
                    # --- Cartões / anti-duplicidade e parcelamento
        is_cartao = (pagamento in CARTOES)

        pagto_fatura = False
        if is_cartao:
            pagto_fatura = st.checkbox(
                "Este lançamento é PAGAMENTO DA FATURA do cartão? (não entra no gasto do mês)",
                value=False
            )

        parcelado = False
        parcelas = 1
        primeira_parcela = data_lcto
        dia_parcela = None

        if is_cartao and (not pagto_fatura):
            parcelado = st.checkbox("Compra parcelada?", value=False)

            if parcelado:
                parcelas = st.number_input("Parcelas (x)", min_value=2, max_value=36, step=1, value=2)
                primeira_parcela = st.date_input("Data da 1ª parcela", value=data_lcto)
                dia_parcela = st.number_input("Dia das parcelas (não usar 1)", min_value=1, max_value=31, step=1, value=int(primeira_parcela.day))
            obs = st.text_input("Observação")
            salvar = st.form_submit_button("Salvar gasto")

    if salvar:
        origem = ""
        if (pagamento in CARTOES) and ("pagto_fatura" in locals()) and pagto_fatura:
            origem = "PAGTO_FATURA"

        novo = {
            "ID": uuid.uuid4().hex,
            "Data": data_lcto,
            "Categoria": categoria,
            "Subcategoria": sub,
            "Valor": float(valor),
            "Pagamento": pagamento,
            "Quem": quem,
            "Obs": obs,
            "Origem": origem,
            "RefFixa": "",
        }

        # Se for parcelado (cartão), gera projeção para meses seguintes
        if (pagamento in CARTOES) and ("parcelado" in locals()) and parcelado and (not pagto_fatura) and int(parcelas) > 1:
            df_gastos = gerar_lancamentos_parcelados(
                df_gastos=df_gastos,
                base=novo,
                parcelas=int(parcelas),
                primeira_data=primeira_parcela,
                dia_parcela=int(dia_parcela) if dia_parcela is not None else None,
            )
        else:
            df_gastos = pd.concat([df_gastos, pd.DataFrame([novo])], ignore_index=True)

        salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
        st.cache_data.clear()
        st.rerun()


# -----------------------------
# RESUMO
# -----------------------------
elif menu == "Resumo":
    css_metric_compacto()

    st.subheader(f"Resumo: {MES_NOME[mes_sel]}/{ano_sel}")
    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)
        # Remove pagamentos de fatura do cálculo (evita duplicidade)
    df_periodo = df_periodo.loc[~df_periodo["Origem"].astype(str).str.upper().isin([x.upper() for x in ORIGENS_NAO_CONTABILIZAR])].copy()
    df_periodo, _ = _ensure_gastos_schema(df_periodo)


    total_fixas_planejado, total_fixas_lancado, df_variaveis, df_fix_mes, f_ativas = marcar_e_separar_fixas(df_periodo, df_fixas)

    gasto_variavel_mes = float(df_variaveis["Valor"].sum()) if not df_variaveis.empty else 0.0
    total_prev_mes = gasto_variavel_mes + float(total_fixas_planejado)

    meta_geral = get_meta_geral(df_metas)
    perc_meta_geral = (total_prev_mes / meta_geral * 100.0) if meta_geral > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto lançado no mês (variável)", fmt_brl(gasto_variavel_mes))
    c2.metric("Fixas do mês (ativas)", fmt_brl(total_fixas_planejado))
    c3.metric("Total previsto (mês)", fmt_brl(total_prev_mes))
    c4.metric("% Meta Geral", fmt_pct(perc_meta_geral) if meta_geral > 0 else "—")

    st.caption(f"Fixas lançadas no mês (valor real): {fmt_brl(total_fixas_lancado)}")

    if meta_geral > 0:
        st.progress(min(total_prev_mes / meta_geral, 1.0))

    st.divider()

    # Por categoria (somente variáveis)
    if df_variaveis.empty:
        st.subheader("Por categoria (variável)")
        st.info("Sem lançamentos variáveis no período.")
    else:
        resumo_cat = (
            df_variaveis.groupby("Categoria")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        itens = [{"label": r["Categoria"], "valor": float(r["Valor"])} for _, r in resumo_cat.iterrows()]
        render_barras_linhas(itens, "Por categoria (variável)", total_base=float(resumo_cat["Valor"].sum()))

    # Por pagamento (somente variáveis)
    if df_variaveis.empty:
        st.subheader("Por forma de pagamento (variável)")
        st.info("Sem lançamentos variáveis no período.")
    else:
        resumo_pag = (
            df_variaveis.groupby("Pagamento")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        itens = [{"label": r["Pagamento"], "valor": float(r["Valor"])} for _, r in resumo_pag.iterrows()]
        render_barras_linhas(itens, "Por forma de pagamento (variável)", total_base=float(resumo_pag["Valor"].sum()))

# -----------------------------
# GERENCIAR (editar / apagar sem mostrar IDs)
# -----------------------------
elif menu == "Gerenciar":
    css_metric_compacto()
    st.subheader("Gerenciar lançamentos (editar / apagar)")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)

    col1, col2 = st.columns([1, 1])
    with col1:
        editar_todos = st.checkbox("Editar todos (não só o período)", value=False)
    with col2:
        incluir_fixas = st.checkbox("Incluir lançamentos de contas fixas", value=True)

    base = df_gastos.copy() if editar_todos else df_periodo.copy()
    if not incluir_fixas:
        _, _, base, _, _ = marcar_e_separar_fixas(base, df_fixas)

    if base.empty:
        st.info("Sem lançamentos para este filtro.")
    else:
        view = base.copy()
        view["Data_dt"] = pd.to_datetime(view["Data"], errors="coerce")
        view = view.sort_values("Data_dt", ascending=False).copy()

        def rotulo(r):
            d = pd.to_datetime(r["Data"], errors="coerce")
            dstr = d.strftime("%d/%m/%Y") if pd.notna(d) else ""
            return f"{dstr} | {r['Categoria']} | {fmt_brl(r['Valor'])} | {r['Pagamento']} | {r['Quem']} | {r['Subcategoria']}"

        view["Rotulo"] = view.apply(rotulo, axis=1)

        escolha_id = st.selectbox(
            "Selecione um lançamento",
            options=view["ID"].tolist(),
            format_func=lambda x: view.loc[view["ID"] == x, "Rotulo"].iloc[0]
        )

        linha = df_gastos.loc[df_gastos["ID"] == escolha_id].iloc[0].to_dict()

        st.divider()
        st.subheader("Editar lançamento")
        with st.form("edit_lcto"):
            data2 = st.date_input("Data", value=pd.to_datetime(linha["Data"], errors="coerce").date() if linha["Data"] else date.today())
            cat2 = st.text_input("Categoria", value=str(linha["Categoria"]))
            sub2 = st.text_input("Subcategoria", value=str(linha["Subcategoria"]))
            val2 = st.number_input("Valor (R$)", min_value=0.0, step=1.0, value=float(linha["Valor"]))
            pag2 = st.selectbox("Forma de pagamento", PAGAMENTOS, index=PAGAMENTOS.index(linha["Pagamento"]) if linha["Pagamento"] in PAGAMENTOS else 0)
            qm2 = st.selectbox("Quem", PESSOAS, index=PESSOAS.index(linha["Quem"]) if linha["Quem"] in PESSOAS else 0)
            obs2 = st.text_input("Obs", value=str(linha["Obs"]))
            salvar2 = st.form_submit_button("Salvar edição")

        if salvar2:
            df_gastos.loc[df_gastos["ID"] == escolha_id, ["Data","Categoria","Subcategoria","Valor","Pagamento","Quem","Obs"]] = \
                [data2, cat2, sub2, float(val2), pag2, qm2, obs2]
            df_gastos, _ = _ensure_gastos_schema(df_gastos)
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.subheader("Excluir lançamento")
        confirmar = st.checkbox("Confirmo a exclusão definitiva", value=False)
        if st.button("Excluir selecionado", type="primary", disabled=not confirmar):
            df_gastos = df_gastos.loc[df_gastos["ID"] != escolha_id].copy()
            df_gastos, _ = _ensure_gastos_schema(df_gastos)
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

# -----------------------------
# METAS DE GASTOS
# -----------------------------
elif menu == "Metas de gastos":
    css_metric_compacto()
    st.subheader(f"Metas de gastos: {MES_NOME[mes_sel]}/{ano_sel}")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)
        # Remove pagamentos de fatura do cálculo (evita duplicidade)
    df_periodo = df_periodo.loc[~df_periodo["Origem"].astype(str).str.upper().isin([x.upper() for x in ORIGENS_NAO_CONTABILIZAR])].copy()
    df_periodo, _ = _ensure_gastos_schema(df_periodo)

    total_fixas_planejado, total_fixas_lancado, df_variaveis, df_fix_mes, f_ativas = marcar_e_separar_fixas(df_periodo, df_fixas)

    gasto_variavel = float(df_variaveis["Valor"].sum()) if not df_variaveis.empty else 0.0
    total_prev = gasto_variavel + float(total_fixas_planejado)

    meta_geral = get_meta_geral(df_metas)
    perc = (total_prev / meta_geral * 100.0) if meta_geral > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto variável", fmt_brl(gasto_variavel))
    c2.metric("Fixas (ativas)", fmt_brl(total_fixas_planejado))
    c3.metric("Total previsto", fmt_brl(total_prev))
    c4.metric("% Meta Geral", fmt_pct(perc) if meta_geral > 0 else "—")

    if meta_geral > 0:
        st.progress(min(total_prev / meta_geral, 1.0))

    st.divider()
    st.subheader("Metas por categoria (variáveis)")

    metas_base = df_metas.copy()
    metas_base["Categoria"] = metas_base["Categoria"].astype(str).str.strip()
    metas_base["Meta"] = pd.to_numeric(metas_base["Meta"], errors="coerce").fillna(0.0)
    metas_cat = metas_base[metas_base["Categoria"].str.lower() != "geral"].copy()

    if df_variaveis.empty:
        gasto_por_cat = {}
    else:
        gasto_por_cat = df_variaveis.groupby("Categoria")["Valor"].sum().to_dict()

    # Monta itens em formato "Categoria" com barra usada vs meta
    itens = []
    for _, r in metas_cat.iterrows():
        cat = str(r["Categoria"]).strip()
        meta = float(r["Meta"])
        gasto = float(gasto_por_cat.get(cat, 0.0))
        pct = (gasto / meta * 100.0) if meta > 0 else 0.0
        itens.append({
            "label": cat,
            "valor": gasto,
            "meta": meta,
            "pct": pct
        })

    if not itens:
        st.info("Cadastre metas por categoria em Cadastros.")
    else:
        # Render custom: valor + % e também mostra meta
        st.markdown(
            """
            <style>
            .meta-wrap {margin: 10px 0;}
            .meta-top {display:flex; justify-content:space-between; gap:12px; font-size: 0.95rem;}
            .meta-label {font-weight:600; color:#1f2937;}
            .meta-val {font-variant-numeric: tabular-nums; color:#111827;}
            .meta-sub {font-size:0.85rem; color:#6b7280; margin-top:2px;}
            .meta-bg {width:100%; height: 10px; border-radius:999px; background: rgba(59,130,246,0.15); overflow:hidden; margin-top: 6px;}
            .meta-fill {height:100%; border-radius:999px; background: rgba(59,130,246,0.85);}
            </style>
            """,
            unsafe_allow_html=True
        )

        html = ""
        for x in itens:
            meta = float(x["meta"])
            gasto = float(x["valor"])
            pct = float(x["pct"])
            w = max(0.0, min(pct, 100.0))
            html += f"""
            <div class="meta-wrap">
              <div class="meta-top">
                <div class="meta-label">{x["label"]}</div>
                <div class="meta-val">{fmt_brl(gasto)} | {fmt_pct(pct)}</div>
              </div>
              <div class="meta-sub">Meta: {fmt_brl(meta)}</div>
              <div class="meta-bg"><div class="meta-fill" style="width:{w:.2f}%;"></div></div>
            </div>
            """
        st.markdown(html, unsafe_allow_html=True)

# -----------------------------
# RESERVA
# -----------------------------
elif menu == "Reserva":
    css_metric_compacto()

    st.subheader("Reserva (movimentações e acompanhamento)")

    reservas_ativas = df_reservas[df_reservas["Ativo"] == True].copy().sort_values("Reserva")
    if reservas_ativas.empty:
        st.info("Não há reservas ativas. Cadastre/ative em Cadastros.")
    else:
        with st.form("mov_reserva"):
            data_mov = st.date_input("Data", date.today())
            rid = st.selectbox(
                "Reserva",
                reservas_ativas["ID_Reserva"].tolist(),
                format_func=lambda x: reservas_ativas.loc[reservas_ativas["ID_Reserva"] == x, "Reserva"].iloc[0]
            )
            tipo = st.selectbox("Tipo", ["Aporte", "Retirada"])
            valor = st.number_input("Valor (R$)", min_value=0.0, step=50.0)
            quem = st.selectbox("Quem", PESSOAS)
            obs = st.text_input("Observação")
            salvar_mov = st.form_submit_button("Salvar movimentação")

        if salvar_mov:
            nome_res = reservas_ativas.loc[reservas_ativas["ID_Reserva"] == rid, "Reserva"].iloc[0]
            novo_mov = {
                "ID_Mov": uuid.uuid4().hex,
                "Data": data_mov,
                "ID_Reserva": str(rid),
                "Reserva": str(nome_res),
                "Tipo": str(tipo),
                "Valor": float(valor),
                "Quem": str(quem),
                "Obs": str(obs),
            }
            df_mov_res = pd.concat([df_mov_res, pd.DataFrame([novo_mov])], ignore_index=True)
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

    st.divider()

    rcalc = calcular_saldos_reservas(df_reservas, df_mov_res)
    rcalc = rcalc[rcalc["Ativo"] == True].copy()

    total_saldo = float(rcalc["Saldo"].sum()) if not rcalc.empty else 0.0
    total_meta = float(rcalc["Meta"].sum()) if not rcalc.empty else 0.0
    perc_total = (total_saldo / total_meta * 100.0) if total_meta > 0 else 0.0
    falta_total = max(total_meta - total_saldo, 0.0)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total reservado", fmt_brl(total_saldo))
    c2.metric("Meta total", fmt_brl(total_meta))
    c3.metric("Falta", fmt_brl(falta_total))
    c4.metric("% atingido", fmt_pct(perc_total) if total_meta > 0 else "—")

    if total_meta > 0:
        st.progress(min(total_saldo / total_meta, 1.0))

    st.divider()
    st.subheader("Detalhe por reserva")
    if rcalc.empty:
        st.info("Sem reservas ativas.")
    else:
        itens = [{"label": r["Reserva"], "valor": float(r["Saldo"])} for _, r in rcalc.sort_values("Saldo", ascending=False).iterrows()]
        # % é baseado na meta total (visão global)
        render_barras_linhas(itens, "Saldo por reserva (comparativo)", total_base=max(total_meta, 0.0))

        # tabela compacta sem IDs
        view = rcalc[["Reserva", "Meta", "Saldo", "Falta", "Percentual"]].copy()
        view["Meta"] = view["Meta"].map(fmt_brl)
        view["Saldo"] = view["Saldo"].map(fmt_brl)
        view["Falta"] = view["Falta"].map(fmt_brl)
        view["Percentual"] = view["Percentual"].map(fmt_pct)
        st.dataframe(view, use_container_width=True)

    st.divider()
    st.subheader("Últimas movimentações")
    mov_show = df_mov_res.copy()
    if not mov_show.empty:
        mov_show["Data_dt"] = pd.to_datetime(mov_show["Data"], errors="coerce")
        mov_show = mov_show.sort_values("Data_dt", ascending=False).head(40).copy()
        mov_show["Valor"] = mov_show["Valor"].map(fmt_brl)
        st.dataframe(mov_show[["Data", "Reserva", "Tipo", "Valor", "Quem", "Obs"]], use_container_width=True)
    else:
        st.info("Sem movimentações ainda.")

# -----------------------------
# CONTAS FIXAS (somente visual)
# -----------------------------
elif menu == "Contas Fixas":
    css_metric_compacto()
    st.subheader("Contas fixas (cadastro em Cadastros / lançamento em Lançar)")

    fixas_view = fixas_ativas(df_fixas).sort_values(["Dia_Venc", "Descricao"])
    if fixas_view.empty:
        st.info("Sem contas fixas ativas. Cadastre em Cadastros.")
    else:
        fixas_show = fixas_view.copy()
        fixas_show["Valor"] = fixas_show["Valor"].map(fmt_brl)
        st.dataframe(
            fixas_show[["Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]],
            use_container_width=True
        )
        st.caption("Para lançar (com valor real), use Menu > Lançar > Contas fixas do mês.")

# -----------------------------
# CADASTROS
# -----------------------------
elif menu == "Cadastros":
    css_metric_compacto()
    st.subheader("Cadastros (metas, contas fixas e reservas)")

    tab1, tab2, tab3 = st.tabs(["Metas", "Contas Fixas", "Reservas"])

    # -------- METAS
    with tab1:
        st.subheader("Metas")
        meta_geral_atual = get_meta_geral(df_metas)

        with st.form("form_meta_geral"):
            novo_meta_geral = st.number_input("Meta Geral (R$)", min_value=0.0, step=100.0, value=float(meta_geral_atual))
            salvar_mg = st.form_submit_button("Salvar Meta Geral")
        if salvar_mg:
            df_metas = set_meta_geral(df_metas, float(novo_meta_geral))
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.caption("Edite metas por categoria. (A linha 'Geral' é a meta geral.)")

        metas_edit = df_metas.copy().sort_values("Categoria")
        metas_edit = st.data_editor(
            metas_edit,
            num_rows="dynamic",
            use_container_width=True,
            key="editor_metas_cad"
        )

        if st.button("Salvar metas por categoria", key="btn_save_metas_cad", type="primary"):
            metas_edit, _ = _ensure_metas_schema(metas_edit)
            df_metas = metas_edit
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

    # -------- CONTAS FIXAS
    with tab2:
        st.subheader("Contas Fixas")
        st.caption("Cadastre/edite/apague aqui. (ID oculto.)")

        fixas_tbl = df_fixas.copy()
        if fixas_tbl.empty:
            st.info("Nenhuma conta fixa cadastrada.")
        else:
            fixas_tbl_show = fixas_tbl.copy()
            fixas_tbl_show["Valor"] = fixas_tbl_show["Valor"].map(fmt_brl)
            st.dataframe(
                fixas_tbl_show[["Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]],
                use_container_width=True
            )

        st.divider()
        st.subheader("Adicionar conta fixa")
        with st.form("add_fixa"):
            desc = st.text_input("Descrição")
            cat = st.selectbox("Categoria", cats_lanc + ["Outros"])
            val = st.number_input("Valor padrão (R$)", min_value=0.0, step=1.0)
            dia = st.number_input("Dia de vencimento (1 a 31)", min_value=1, max_value=31, step=1, value=5)
            pag = st.selectbox("Forma de pagamento", PAGAMENTOS)
            quem = st.selectbox("Responsável", PESSOAS)
            ativo = st.checkbox("Ativo", value=True)
            obs = st.text_input("Observação")
            add = st.form_submit_button("Adicionar")
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
            df_fixas, _ = _ensure_fixas_schema(df_fixas)
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.subheader("Editar / apagar conta fixa")
        if df_fixas.empty:
            st.info("Cadastre uma conta fixa primeiro.")
        else:
            opcoes = df_fixas.copy()
            opcoes["Rotulo"] = opcoes.apply(
                lambda r: f"{r['Descricao']} | {fmt_brl(r['Valor'])} | venc {r['Dia_Venc']} | {'Ativo' if r['Ativo'] else 'Inativo'}",
                axis=1
            )
            escolha = st.selectbox("Selecione", opcoes["ID_Fixa"].tolist(), format_func=lambda x: opcoes.loc[opcoes["ID_Fixa"] == x, "Rotulo"].iloc[0])

            row = df_fixas.loc[df_fixas["ID_Fixa"] == escolha].iloc[0].to_dict()

            with st.form("edit_fixa"):
                desc2 = st.text_input("Descrição", value=str(row["Descricao"]))
                cat2 = st.selectbox("Categoria", cats_lanc + ["Outros"], index=(cats_lanc + ["Outros"]).index(row["Categoria"]) if row["Categoria"] in (cats_lanc + ["Outros"]) else 0)
                val2 = st.number_input("Valor padrão (R$)", min_value=0.0, step=1.0, value=float(row["Valor"]))
                dia2 = st.number_input("Dia de vencimento", min_value=1, max_value=31, step=1, value=int(row["Dia_Venc"]))
                pag2 = st.selectbox("Pagamento", PAGAMENTOS, index=PAGAMENTOS.index(row["Pagamento"]) if row["Pagamento"] in PAGAMENTOS else 0)
                quem2 = st.selectbox("Quem", PESSOAS, index=PESSOAS.index(row["Quem"]) if row["Quem"] in PESSOAS else 0)
                ativo2 = st.checkbox("Ativo", value=bool(row["Ativo"]))
                obs2 = st.text_input("Obs", value=str(row["Obs"]))
                salvar2 = st.form_submit_button("Salvar edição")
            if salvar2:
                df_fixas.loc[df_fixas["ID_Fixa"] == escolha, ["Descricao","Categoria","Valor","Dia_Venc","Pagamento","Quem","Ativo","Obs"]] = \
                    [desc2.strip(), cat2.strip(), float(val2), int(dia2), pag2, quem2, bool(ativo2), obs2]
                df_fixas, _ = _ensure_fixas_schema(df_fixas)
                salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
                st.cache_data.clear()
                st.rerun()

            st.divider()
            confirmar = st.checkbox("Confirmo apagar esta conta fixa", value=False, key="conf_del_fixa")
            if st.button("Apagar conta fixa selecionada", disabled=not confirmar):
                df_fixas = df_fixas.loc[df_fixas["ID_Fixa"] != escolha].copy()
                df_fixas, _ = _ensure_fixas_schema(df_fixas)
                salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
                st.cache_data.clear()
                st.rerun()

    # -------- RESERVAS
    with tab3:
        st.subheader("Reservas")
        st.caption("Cadastre/edite/apague aqui. (ID oculto.)")

        if df_reservas.empty:
            st.info("Nenhuma reserva cadastrada.")
        else:
            res_tbl = df_reservas.copy()
            res_tbl_show = res_tbl.copy()
            res_tbl_show["Meta"] = res_tbl_show["Meta"].map(fmt_brl)
            st.dataframe(
                res_tbl_show[["Reserva", "Meta", "Ativo", "Obs"]],
                use_container_width=True
            )

        st.divider()
        st.subheader("Adicionar reserva")
        with st.form("add_reserva"):
            nome = st.text_input("Nome da reserva")
            meta = st.number_input("Meta (R$)", min_value=0.0, step=100.0)
            ativo = st.checkbox("Ativo", value=True, key="ativo_add_res")
            obs = st.text_input("Observação", key="obs_add_res")
            add = st.form_submit_button("Adicionar")
        if add:
            novo = {
                "ID_Reserva": uuid.uuid4().hex,
                "Reserva": nome.strip(),
                "Meta": float(meta),
                "Ativo": bool(ativo),
                "Obs": obs
            }
            df_reservas = pd.concat([df_reservas, pd.DataFrame([novo])], ignore_index=True)
            df_reservas, _ = _ensure_reservas_schema(df_reservas)
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.subheader("Editar / apagar reserva")
        if df_reservas.empty:
            st.info("Cadastre uma reserva primeiro.")
        else:
            op = df_reservas.copy()
            op["Rotulo"] = op.apply(lambda r: f"{r['Reserva']} | meta {fmt_brl(r['Meta'])} | {'Ativo' if r['Ativo'] else 'Inativo'}", axis=1)
            escolha = st.selectbox("Selecione", op["ID_Reserva"].tolist(), format_func=lambda x: op.loc[op["ID_Reserva"] == x, "Rotulo"].iloc[0])

            row = df_reservas.loc[df_reservas["ID_Reserva"] == escolha].iloc[0].to_dict()

            with st.form("edit_reserva"):
                nome2 = st.text_input("Nome", value=str(row["Reserva"]))
                meta2 = st.number_input("Meta (R$)", min_value=0.0, step=100.0, value=float(row["Meta"]))
                ativo2 = st.checkbox("Ativo", value=bool(row["Ativo"]))
                obs2 = st.text_input("Obs", value=str(row["Obs"]))
                salvar2 = st.form_submit_button("Salvar edição")
            if salvar2:
                df_reservas.loc[df_reservas["ID_Reserva"] == escolha, ["Reserva","Meta","Ativo","Obs"]] = \
                    [nome2.strip(), float(meta2), bool(ativo2), obs2]
                df_reservas, _ = _ensure_reservas_schema(df_reservas)
                salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
                st.cache_data.clear()
                st.rerun()

            st.divider()
            confirmar = st.checkbox("Confirmo apagar esta reserva", value=False, key="conf_del_res")
            if st.button("Apagar reserva selecionada", disabled=not confirmar):
                df_reservas = df_reservas.loc[df_reservas["ID_Reserva"] != escolha].copy()
                df_mov_res = df_mov_res.loc[df_mov_res["ID_Reserva"] != str(escolha)].copy()

                df_reservas, _ = _ensure_reservas_schema(df_reservas)
                df_mov_res, _ = _ensure_movres_schema(df_mov_res)

                salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
                st.cache_data.clear()
                st.rerun()

# -----------------------------
# BACKUP / RESTORE
# -----------------------------
else:
    css_metric_compacto()
    st.subheader("Backup / Restore")

    if "backup_bytes" not in st.session_state:
        st.session_state["backup_bytes"] = None

    if st.button("Gerar backup Excel"):
        st.session_state["backup_bytes"] = excel_bytes(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res)

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
        df_gastos, df_metas, df_fixas, df_reservas, df_mov_res = restore_from_upload(up)
        st.success("Backup restaurado.")
        st.rerun()






