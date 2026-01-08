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
# UI: METRIC FONT + BARRA (linha azul com valor/% no fim)
# -----------------------------
def set_metric_font(value_rem: str = "1.35rem", label_rem: str = "0.85rem"):
    st.markdown(
        f"""
        <style>
        div[data-testid="stMetricValue"] {{ font-size: {value_rem} !important; }}
        div[data-testid="stMetricLabel"] {{ font-size: {label_rem} !important; }}
        </style>
        """,
        unsafe_allow_html=True
    )

def inject_progress_css():
    st.markdown(
        """
        <style>
        .ttbar{margin:0.25rem 0 0.75rem 0;}
        .ttbar-row{display:flex;justify-content:space-between;gap:0.75rem;font-size:0.90rem;line-height:1.2;}
        .ttbar-label{font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:70%;}
        .ttbar-right{font-variant-numeric:tabular-nums;white-space:nowrap;}
        .ttbar-track{width:100%;height:10px;border-radius:999px;background:rgba(49,51,63,0.15);overflow:hidden;}
        .ttbar-fill{height:10px;background:var(--primary-color);border-radius:999px;}
        </style>
        """,
        unsafe_allow_html=True
    )

def progress_line(label: str, value: float, total: float, right_text: str | None = None):
    try:
        v = float(value)
    except Exception:
        v = 0.0
    try:
        t = float(total)
    except Exception:
        t = 0.0

    pct = 0.0
    if t > 0:
        pct = max(0.0, min(v / t, 1.0))

    if right_text is None:
        right_text = f"{fmt_brl(v)} | {fmt_pct(pct * 100.0)}"

    st.markdown(
        f"""
        <div class="ttbar">
          <div class="ttbar-row">
            <div class="ttbar-label">{label}</div>
            <div class="ttbar-right">{right_text}</div>
          </div>
          <div class="ttbar-track">
            <div class="ttbar-fill" style="width:{pct*100.0:.1f}%"></div>
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

inject_progress_css()

# -----------------------------
# COLUNAS / SCHEMAS
# -----------------------------
GASTOS_COLS = ["ID", "Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs", "Origem", "RefFixa"]
METAS_COLS = ["Categoria", "Meta"]
FIXAS_COLS = ["ID_Fixa", "Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]

RESERVAS_COLS = ["ID_Reserva", "Reserva", "Meta", "Ativo", "Obs"]
MOVRES_COLS = ["ID_Mov", "Data", "ID_Reserva", "Reserva", "Tipo", "Valor", "Quem", "Obs"]

# -----------------------------
# LOCK
# -----------------------------
def acquire_lock(lock_path: str, stale_seconds: int = 180, timeout_seconds: int = 5) -> bool:
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
        ("Reserva Saúde", 0, True, "Ex.: remédios, consultas, exames."),
        ("Reserva Casa", 0, True, "Manutenção, móveis, imprevistos."),
        ("Reserva Veículo/Transporte", 0, True, "IPVA, pneus, revisões, multas."),
        ("Reserva Impostos/Taxas", 0, True, "Ex.: tributos, taxas anuais."),
        ("Reserva Viagens/Lazer", 0, True, "Planejamento de férias/viagens."),
        ("Reserva Educação", 0, True, "Cursos, certificações, livros."),
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
    mov, chg_mov = _ensure_movres_schema(res(res["mov_res"]) if False else res["mov_res"])  # placeholder
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

def separar_fixas(df_periodo: pd.DataFrame, df_fixas: pd.DataFrame):
    fixas_ativas = df_fixas[df_fixas["Ativo"] == True].copy()
    fixas_total = float(fixas_ativas["Valor"].sum()) if not fixas_ativas.empty else 0.0

    if df_periodo.empty or fixas_ativas.empty:
        return fixas_total, 0.0, fixas_total, df_periodo.copy()

    ids_fixas = set(fixas_ativas["ID_Fixa"].astype(str).str.strip().tolist())
    cats_fixas = set(fixas_ativas["Categoria"].astype(str).str.strip().tolist())
    desc_fixas = set(fixas_ativas["Descricao"].astype(str).str.strip().tolist())

    g = df_periodo.copy()
    g["Origem"] = g["Origem"].astype(str)
    g["RefFixa"] = g["RefFixa"].astype(str)
    g["Categoria"] = g["Categoria"].astype(str).str.strip()
    g["Subcategoria"] = g["Subcategoria"].astype(str).str.strip()

    is_fixa = (
        (g["Origem"].str.upper() == "FIXA") |
        (g["RefFixa"].str.strip().isin(ids_fixas)) |
        ((g["Categoria"] != "") & (g["Categoria"].isin(cats_fixas))) |
        ((g["Subcategoria"] != "") & (g["Subcategoria"].isin(desc_fixas)))
    )

    fixas_lancadas = float(g.loc[is_fixa, "Valor"].sum())
    fixas_restantes = max(fixas_total - fixas_lancadas, 0.0)

    variaveis = g.loc[~is_fixa].copy()
    variaveis, _ = _ensure_gastos_schema(variaveis)

    return fixas_total, fixas_lancadas, fixas_restantes, variaveis

def gerar_lancamentos_fixas(df_gastos: pd.DataFrame, df_fixas: pd.DataFrame, ano: int, mes: int):
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
        ["Lançar", "Resumo", "Gerenciar", "Metas", "Reserva", "Contas Fixas", "Cadastros", "Backup/Restore"],
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
        salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("Últimos lançamentos")
    show = df_gastos.copy()
    show["Data_dt"] = pd.to_datetime(show["Data"], errors="coerce")
    show = show.sort_values("Data_dt", ascending=False).head(30).copy()
    show["Valor"] = show["Valor"].map(fmt_brl)
    st.dataframe(
        show[["Data", "Categoria", "Subcategoria", "Valor", "Pagamento", "Quem", "Obs"]],
        use_container_width=True
    )

# -----------------------------
# RESUMO
# -----------------------------
elif menu == "Resumo":
    set_metric_font("1.35rem", "0.85rem")

    st.subheader(f"Resumo: {MES_NOME[mes_sel]}/{ano_sel}")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)

    fixas_total, fixas_lancadas, fixas_restantes, df_variaveis = separar_fixas(df_periodo, df_fixas)

    gasto_variavel_mes = float(df_variaveis["Valor"].sum()) if not df_variaveis.empty else 0.0
    total_prev = gasto_variavel_mes + fixas_restantes

    meta_geral = get_meta_geral(df_metas)
    perc_meta_geral = (total_prev / meta_geral * 100.0) if meta_geral > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto lançado no mês (sem fixas)", fmt_brl(gasto_variavel_mes))
    c2.metric("Fixas previstas (restantes)", fmt_brl(fixas_restantes))
    c3.metric("Total previsto (mês)", fmt_brl(total_prev))
    c4.metric("% Meta Geral", fmt_pct(perc_meta_geral) if meta_geral > 0 else "—")

    st.caption(f"Fixas ativas do mês: {fmt_brl(fixas_total)} | Já lançadas/pagas (estimado): {fmt_brl(fixas_lancadas)}")

    # Linha azul com valor/% no fim (Meta geral)
    if meta_geral > 0:
        progress_line(
            "Meta geral (projeção do mês)",
            total_prev,
            meta_geral,
            right_text=f"{fmt_brl(total_prev)} | {fmt_pct(perc_meta_geral)}"
        )

    st.divider()
    st.subheader("Por categoria (lançado - sem fixas)")
    if df_variaveis.empty:
        st.info("Sem lançamentos variáveis no período.")
    else:
        resumo_cat = (
            df_variaveis.groupby("Categoria")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        total_cat = float(resumo_cat["Valor"].sum())

        # Barras (linha) com valor/% no fim
        for _, r in resumo_cat.iterrows():
            v = float(r["Valor"])
            pct = (v / total_cat * 100.0) if total_cat > 0 else 0.0
            progress_line(
                str(r["Categoria"]),
                v,
                total_cat,
                right_text=f"{fmt_brl(v)} | {fmt_pct(pct)}"
            )

        # Mantém tabela como estava
        resumo_cat["Percentual"] = (resumo_cat["Valor"] / total_cat * 100.0) if total_cat > 0 else 0.0
        resumo_cat_disp = resumo_cat.copy()
        resumo_cat_disp["Valor"] = resumo_cat_disp["Valor"].map(fmt_brl)
        resumo_cat_disp["Percentual"] = resumo_cat_disp["Percentual"].map(fmt_pct)
        st.dataframe(resumo_cat_disp[["Categoria", "Valor", "Percentual"]], use_container_width=True)

    st.subheader("Por pessoa (lançado - sem fixas)")
    if not df_variaveis.empty:
        resumo_pessoa = (
            df_variaveis.groupby("Quem")["Valor"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
        )
        resumo_pessoa["Valor"] = resumo_pessoa["Valor"].map(fmt_brl)
        st.dataframe(resumo_pessoa, use_container_width=True)

# -----------------------------
# GERENCIAR (Lançamentos)
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
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.subheader("Excluir lançamento rápido")
        df_sel = df_view.copy()
        df_sel["DataStr"] = pd.to_datetime(df_sel["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
        df_sel["Rotulo"] = df_sel.apply(
            lambda r: f"{r['DataStr']} | {r['Categoria']} | {fmt_brl(r['Valor'])} | {r['Quem']} | ID={r['ID']}",
            axis=1,
        )
        escolha = st.selectbox("Selecione", df_sel["Rotulo"].tolist())
        confirmar = st.checkbox("Confirmo a exclusão definitiva", value=False)

        if st.button("Excluir selecionado") and confirmar:
            id_escolhido = escolha.split("ID=")[-1].strip()
            df_gastos = df_gastos.loc[df_gastos["ID"].astype(str) != id_escolhido].copy()
            salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
            st.cache_data.clear()
            st.rerun()

# -----------------------------
# METAS (somente visual)
# -----------------------------
elif menu == "Metas":
    set_metric_font("1.35rem", "0.85rem")

    st.subheader(f"Metas: {MES_NOME[mes_sel]}/{ano_sel}")

    df_periodo = filtro_periodo_gastos(df_gastos, ano_sel, mes_sel)
    fixas_total, fixas_lancadas, fixas_restantes, df_variaveis = separar_fixas(df_periodo, df_fixas)

    gasto_total = float(df_variaveis["Valor"].sum()) if not df_variaveis.empty else 0.0
    total_prev = gasto_total + fixas_restantes

    meta_geral = get_meta_geral(df_metas)
    perc = (total_prev / meta_geral * 100.0) if meta_geral > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Gasto variável", fmt_brl(gasto_total))
    c2.metric("Fixas restantes", fmt_brl(fixas_restantes))
    c3.metric("Total previsto", fmt_brl(total_prev))
    c4.metric("% Meta Geral", fmt_pct(perc) if meta_geral > 0 else "—")

    # Linha azul com valor/% no fim (Meta geral)
    if meta_geral > 0:
        progress_line(
            "Meta geral (projeção do mês)",
            total_prev,
            meta_geral,
            right_text=f"{fmt_brl(total_prev)} | {fmt_pct(perc)}"
        )

    st.divider()
    st.subheader("Metas por categoria (variáveis)")
    gasto_por_cat = df_variaveis.groupby("Categoria")["Valor"].sum().to_dict() if not df_variaveis.empty else {}

    metas_base = df_metas.copy()
    metas_base["Categoria"] = metas_base["Categoria"].astype(str).str.strip()
    metas_base["Meta"] = pd.to_numeric(metas_base["Meta"], errors="coerce").fillna(0.0)

    metas_cat = metas_base[metas_base["Categoria"].str.lower() != "geral"].copy()
    metas_cat["Gasto_Mes"] = metas_cat["Categoria"].map(lambda c: float(gasto_por_cat.get(str(c).strip(), 0.0)))
    metas_cat["Falta"] = (metas_cat["Meta"] - metas_cat["Gasto_Mes"]).clip(lower=0.0)
    metas_cat["Excedeu"] = (metas_cat["Gasto_Mes"] - metas_cat["Meta"]).clip(lower=0.0)
    metas_cat["Percentual_Usado"] = metas_cat.apply(lambda r: (r["Gasto_Mes"] / r["Meta"] * 100.0) if r["Meta"] > 0 else 0.0, axis=1)

    # Barras (linha) com valor/% no fim (por meta da categoria)
    metas_cat_ord = metas_cat.sort_values("Percentual_Usado", ascending=False).copy()
    for _, r in metas_cat_ord.iterrows():
        cat = str(r["Categoria"])
        meta_v = float(r["Meta"])
        gasto_v = float(r["Gasto_Mes"])
        pct_v = float(r["Percentual_Usado"])
        right = f"{fmt_brl(gasto_v)} / {fmt_brl(meta_v)} | {fmt_pct(pct_v)}"
        progress_line(cat, gasto_v, meta_v if meta_v > 0 else 0.0, right_text=right)

    # Mantém tabela como estava
    view = metas_cat[["Categoria", "Meta", "Gasto_Mes", "Falta", "Excedeu", "Percentual_Usado"]].copy()
    view["Meta"] = view["Meta"].map(fmt_brl)
    view["Gasto_Mes"] = view["Gasto_Mes"].map(fmt_brl)
    view["Falta"] = view["Falta"].map(fmt_brl)
    view["Excedeu"] = view["Excedeu"].map(fmt_brl)
    view["Percentual_Usado"] = view["Percentual_Usado"].map(fmt_pct)
    st.dataframe(view, use_container_width=True)

# -----------------------------
# RESERVA (sem editar cadastro aqui)
# -----------------------------
elif menu == "Reserva":
    set_metric_font("1.25rem", "0.85rem")

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

    # Linha azul com valor/% no fim (total reservas)
    if total_meta > 0:
        progress_line(
            "Progresso total das reservas",
            total_saldo,
            total_meta,
            right_text=f"{fmt_brl(total_saldo)} / {fmt_brl(total_meta)} | {fmt_pct(perc_total)}"
        )

    st.subheader("Detalhe por reserva")
    if rcalc.empty:
        st.info("Sem reservas ativas.")
    else:
        # Barras (linha) com valor/% no fim (por reserva)
        rcalc_ord = rcalc.sort_values("Percentual", ascending=False).copy()
        for _, r in rcalc_ord.iterrows():
            nome = str(r["Reserva"])
            meta_v = float(r["Meta"])
            saldo_v = float(r["Saldo"])
            pct_v = float(r["Percentual"])
            right = f"{fmt_brl(saldo_v)} / {fmt_brl(meta_v)} | {fmt_pct(pct_v)}"
            progress_line(nome, saldo_v, meta_v if meta_v > 0 else 0.0, right_text=right)

        # Mantém tabela como estava
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
# CONTAS FIXAS (sem editar aqui)
# -----------------------------
elif menu == "Contas Fixas":
    st.subheader("Contas fixas (visual e geração do mês)")

    fixas_view = df_fixas.copy()
    if fixas_view.empty:
        st.info("Sem contas fixas. Cadastre em Cadastros.")
    else:
        fixas_view["Valor"] = fixas_view["Valor"].map(fmt_brl)
        st.dataframe(
            fixas_view[["Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]],
            use_container_width=True
        )

    st.divider()
    st.subheader(f"Gerar lançamentos das contas fixas em {MES_NOME[mes_sel]}/{ano_sel}")
    st.caption("Cria lançamentos em 'gastos' para as fixas ativas, sem duplicar no mês.")

    if st.button("Gerar lançamentos do mês", type="primary"):
        df_gastos_novo, criados, ignorados = gerar_lancamentos_fixas(df_gastos, df_fixas, ano_sel, mes_sel)
        df_gastos = df_gastos_novo
        salvar_excel(df_gastos, df_metas, df_fixas, df_reservas, df_mov_res, ARQUIVO)
        st.cache_data.clear()
        st.success(f"Lançamentos criados: {criados}. Ignorados (já existiam): {ignorados}.")
        st.rerun()

# -----------------------------
# CADASTROS (ÚNICO lugar de edição)
# -----------------------------
elif menu == "Cadastros":
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
        st.caption("Edite metas por categoria. (A categoria 'Geral' existe separada acima.)")

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
        st.caption("Aqui é o único lugar para criar/editar/apagar contas fixas. (ID fica oculto.)")

        if df_fixas.empty:
            st.info("Nenhuma conta fixa cadastrada.")
        else:
            fixas_tbl = df_fixas.copy()
            fixas_tbl["Valor"] = fixas_tbl["Valor"].map(fmt_brl)
            st.dataframe(
                fixas_tbl[["Descricao", "Categoria", "Valor", "Dia_Venc", "Pagamento", "Quem", "Ativo", "Obs"]],
                use_container_width=True
            )

        st.divider()
        st.subheader("Adicionar conta fixa")
        with st.form("add_fixa"):
            desc = st.text_input("Descrição")
            cat = st.selectbox("Categoria", cats_lanc + ["Outros"])
            val = st.number_input("Valor (R$)", min_value=0.0, step=1.0)
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
            opcoes["Rotulo"] = opcoes.apply(lambda r: f"{r['Descricao']} | {fmt_brl(r['Valor'])} | venc {r['Dia_Venc']} | {'Ativo' if r['Ativo'] else 'Inativo'}", axis=1)
            escolha = st.selectbox("Selecione", opcoes["ID_Fixa"].tolist(), format_func=lambda x: opcoes.loc[opcoes["ID_Fixa"] == x, "Rotulo"].iloc[0])

            row = df_fixas.loc[df_fixas["ID_Fixa"] == escolha].iloc[0].to_dict()

            with st.form("edit_fixa"):
                desc2 = st.text_input("Descrição", value=str(row["Descricao"]))
                cat2 = st.selectbox("Categoria", cats_lanc + ["Outros"], index=(cats_lanc + ["Outros"]).index(row["Categoria"]) if row["Categoria"] in (cats_lanc + ["Outros"]) else 0)
                val2 = st.number_input("Valor (R$)", min_value=0.0, step=1.0, value=float(row["Valor"]))
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
        st.caption("Aqui é o único lugar para criar/editar/apagar reservas. (ID fica oculto.)")

        if df_reservas.empty:
            st.info("Nenhuma reserva cadastrada.")
        else:
            res_tbl = df_reservas.copy()
            res_tbl["Meta"] = res_tbl["Meta"].map(fmt_brl)
            st.dataframe(
                res_tbl[["Reserva", "Meta", "Ativo", "Obs"]],
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
