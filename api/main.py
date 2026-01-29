from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd

app = FastAPI(title="API de Operadoras")

# --- CORS --- #
origins = [
    "http://127.0.0.1:5500",
    "http://localhost:5500",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Caminho CSV --- #
DATA_DIR = Path(__file__).parent.parent / "data"
DESPESAS_PATH = DATA_DIR / "despesas_agregadas.csv"

print("Caminho CSV:", DESPESAS_PATH)
print("CSV existe?", DESPESAS_PATH.exists())

# --- Carrega CSV com prova de falhas --- #
despesas_agregadas = pd.DataFrame()

try:
    if not DESPESAS_PATH.exists():
        raise FileNotFoundError(f"CSV não encontrado: {DESPESAS_PATH}")

    despesas_agregadas = pd.read_csv(DESPESAS_PATH, encoding="utf-8")

    required_cols = ["RegistroANS", "RazaoSocial", "UF", "TotalDespesas"]
    for col in required_cols:
        if col not in despesas_agregadas.columns:
            raise ValueError(f"CSV não contém coluna obrigatória: {col}")

    # Converte TotalDespesas para numérico
    despesas_agregadas["TotalDespesas"] = pd.to_numeric(
        despesas_agregadas["TotalDespesas"], errors="coerce"
    ).fillna(0)

    print("CSV carregado com sucesso! Linhas:", len(despesas_agregadas))

except Exception as e:
    print("Erro ao carregar CSV:", e)

# --- Rotas --- #

@app.get("/api/operadoras")
def listar_operadoras(page: int = 1, limit: int = 50):
    if despesas_agregadas.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")
    start = (page - 1) * limit
    end = start + limit
    data = despesas_agregadas.iloc[start:end][
        ["RegistroANS", "RazaoSocial", "UF", "TotalDespesas"]
    ].to_dict(orient="records")
    return {
        "data": data,
        "page": page,
        "limit": limit,
        "total": len(despesas_agregadas),
    }

@app.get("/api/operadoras/{registro_ans}")
def detalhe_operadora(registro_ans: str):
    df = despesas_agregadas[
        despesas_agregadas["RegistroANS"].astype(str) == registro_ans
    ]
    if df.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    return df.iloc[0][["RegistroANS", "RazaoSocial", "UF", "TotalDespesas"]].to_dict()

@app.get("/api/operadoras/{registro_ans}/despesas")
def historico_despesas(registro_ans: str):
    df = despesas_agregadas[
        despesas_agregadas["RegistroANS"].astype(str) == registro_ans
    ]
    if df.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    # Para este exemplo, retornamos histórico simplificado (pode ser expandido)
    return df[["RegistroANS", "RazaoSocial", "UF", "TotalDespesas", "MediaTrimestral", "DesvioPadrao"]].to_dict(orient="records")

@app.get("/api/estatisticas")
def estatisticas():
    if despesas_agregadas.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")
    total = despesas_agregadas["TotalDespesas"].sum()
    media = despesas_agregadas["TotalDespesas"].mean()
    top_5 = despesas_agregadas.sort_values(
        "TotalDespesas", ascending=False
    ).head(5)
    top_5_list = top_5[
        ["RegistroANS", "RazaoSocial", "UF", "TotalDespesas"]
    ].to_dict(orient="records")
    return {
        "total_despesas": float(total),
        "media_despesas": float(media),
        "top_5_operadoras": top_5_list,
    }
