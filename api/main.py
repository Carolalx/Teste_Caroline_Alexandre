from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import glob
from pathlib import Path

app = FastAPI(title="API de Operadoras")

# --- CONFIGURAÇÃO DE CORS --- #
# Adicionei as portas mais comuns do Live Server (5500 e 5501)
origins = [
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://127.0.0.1:5501",
    "http://localhost:5501",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CAMINHO CSV --- #
# Certifique-se que a pasta 'data' está no local correto
DATA_DIR = Path(__file__).parent.parent / "data"
resultado_despesas_pattern = str(DATA_DIR / "resultado_despesas*.csv")

resultado_despesas = pd.DataFrame()

try:
    resultado_despesas_files = glob.glob(resultado_despesas_pattern)

    if not resultado_despesas_files:
        print(f"AVISO: Nenhum CSV encontrado em {DATA_DIR}")
    else:
        RESULTADO_DESPESAS_PATH = resultado_despesas_files[0]
        print(f"Carregando arquivo: {RESULTADO_DESPESAS_PATH}")

        # Carregando com separador genérico (vírgula ou ponto e vírgula)
        resultado_despesas = pd.read_csv(
            RESULTADO_DESPESAS_PATH, encoding="utf-8", sep=None, engine='python')

        # Normalizar nomes das colunas para minúsculo para evitar erros de digitação
        resultado_despesas.columns = [c.lower().strip()
                                      for c in resultado_despesas.columns]

        # --- CONVERSÃO NUMÉRICA ROBUSTA --- #
        num_cols = ["valordespesas", "totaldespesas",
                    "mediatrimestral", "desviopadrao", "registroans", "ano"]

        for col in num_cols:
            if col in resultado_despesas.columns:
                # Remove pontos de milhar e troca vírgula decimal por ponto
                resultado_despesas[col] = (
                    resultado_despesas[col].astype(str)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .str.strip()
                )
                resultado_despesas[col] = pd.to_numeric(
                    resultado_despesas[col], errors='coerce').fillna(0)

        # Remove lixo ou linhas totalmente vazias
        resultado_despesas = resultado_despesas.dropna(
            subset=["registroans", "razaosocial"])

        print(f"Sucesso! {len(resultado_despesas)} linhas carregadas.")

except Exception as e:
    print(f"ERRO CRÍTICO NO BACKEND: {e}")

# --- ROTAS --- #


@app.get("/api/operadoras")
def listar_operadoras(page: int = 1, limit: int = 50, q: str = None):
    if resultado_despesas.empty:
        return {"data": [], "total": 0}

    df_result = resultado_despesas.copy()

    # BUSCA GLOBAL NO DATAFRAME
    if q:
        q = q.lower()
        # Filtra por Razão Social ou Registro ANS em todo o arquivo CSV
        mask = (
            df_result["razaosocial"].astype(str).str.lower().str.contains(q) |
            df_result["registroans"].astype(str).str.contains(q)
        )
        df_result = df_result[mask]

    total_filtrado = len(df_result)

    # PAGINAÇÃO DOS RESULTADOS FILTRADOS
    start = (page - 1) * limit
    end = start + limit
    df_page = df_result.iloc[start:end]

    data = []
    for _, row in df_page.iterrows():
        data.append({
            "RegistroANS": int(row["registroans"]),
            "RazaoSocial": str(row["razaosocial"]),
            "UF": str(row.get("uf", "N/A")),
            "Ano": int(row["ano"]),
            "Trimestre": str(row["trimestre"]),
            "ValorDespesas": float(row["valordespesas"]),
            "TotalDespesas": float(row["totaldespesas"])
        })

    return {
        "page": page,
        "limit": limit,
        "total": total_filtrado,
        "data": data
    }


@app.get("/api/operadoras/{registro_ans}")
def detalhe_operadora(registro_ans: str):
    # Busca flexível por string para evitar erro de tipo
    mask = resultado_despesas["registroans"].astype(
        int).astype(str) == str(registro_ans)
    df = resultado_despesas[mask]

    if df.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")

    row = df.iloc[0]
    return {
        "RegistroANS": int(row["registroans"]),
        "RazaoSocial": str(row["razaosocial"]),
        "UF": str(row.get("uf", "N/A")),
        "TotalDespesas": float(df["valordespesas"].sum())
    }


@app.get("/api/operadoras/{registro_ans}/despesas")
def historico_despesas(registro_ans: str):
    mask = resultado_despesas["registroans"].astype(
        int).astype(str) == str(registro_ans)
    df = resultado_despesas[mask]

    if df.empty:
        return []

    return [
        {
            "Ano": int(row["ano"]),
            "Trimestre": str(row["trimestre"]),
            "ValorDespesas": float(row["valordespesas"]),
            "MediaTrimestral": float(row.get("mediatrimestral", 0)),
            "DesvioPadrao": float(row.get("desviopadrao", 0))
        }
        for _, row in df.iterrows()
    ]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
