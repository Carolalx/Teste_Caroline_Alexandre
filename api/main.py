from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd

app = FastAPI(title="API de Operadoras")

# --- CORS --- #
origins = [
    "http://127.0.0.1:5501",  # frontend
    "http://localhost:5501",
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
NORMALIZACAO_PATH = DATA_DIR / "normalizacao.csv"

# --- Carrega CSV --- #
normalizacao = pd.DataFrame()

try:
    if not NORMALIZACAO_PATH.exists():
        raise FileNotFoundError(f"CSV não encontrado: {NORMALIZACAO_PATH}")

    # CSV: id, registroans, ano, trimestre, cnpj, razaosocial, modalidade, uf, data_registro_ans, valordespesas, totaldespesas, mediatrimestral, desviopadrao
    normalizacao = pd.read_csv(NORMALIZACAO_PATH, encoding="utf-8")

    # --- Colunas obrigatórias --- #
    required_cols = [
        "id", "registroans", "ano", "trimestre", "cnpj", "razaosocial",
        "modalidade", "uf", "data_registro_ans",
        "valordespesas", "totaldespesas", "mediatrimestral", "desviopadrao"
    ]

    for col in required_cols:
        if col not in normalizacao.columns:
            raise ValueError(f"CSV não contém coluna obrigatória: {col}")

    # --- Converter colunas numéricas corretamente --- #
    num_cols = ["valordespesas", "totaldespesas",
                "mediatrimestral", "desviopadrao"]
    for col in num_cols:
        normalizacao[col] = (
            normalizacao[col].astype(str)
            .str.replace(",", "")  # remove vírgulas de milhar
            .str.replace(" ", "")  # remove espaços
            # valores vazios para 0
            .replace({"": "0", "nan": "0"}, regex=True)
            .astype(float)
        )

    # Remove linhas com valor 0 em valordespesas
    normalizacao = normalizacao[normalizacao["valordespesas"] != 0]

    print(f"CSV carregado com sucesso! Total de linhas: {len(normalizacao)}")

except Exception as e:
    print("Erro ao carregar CSV:", e)


# --- Rotas --- #

@app.get("/api/operadoras")
def listar_operadoras(page: int = 1, limit: int = 50):
    if normalizacao.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")

    start = (page - 1) * limit
    end = start + limit
    df_page = normalizacao.iloc[start:end]

    data = [
        {
            "RegistroANS": int(row["registroans"]),
            "RazaoSocial": str(row["razaosocial"]),
            "UF": str(row["uf"]),
            "Ano": int(row["ano"]),
            "Trimestre": str(row["trimestre"]),
            "ValorDespesas": float(row["valordespesas"]),
            "TotalDespesas": float(row["totaldespesas"]),
            "CNPJ": str(row["cnpj"]),
            "Modalidade": str(row["modalidade"]),
        }
        for _, row in df_page.iterrows()
    ]

    return {
        "page": page,
        "limit": limit,
        "total": len(normalizacao),
        "data": data
    }


@app.get("/api/operadoras/{registro_ans}")
def detalhe_operadora(registro_ans: str):
    df = normalizacao[normalizacao["registroans"].astype(str) == registro_ans]
    if df.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")

    # Retorna o primeiro registro, para detalhes gerais
    row = df.iloc[0]

    return {
        "RegistroANS": int(row["registroans"]),
        "RazaoSocial": str(row["razaosocial"]),
        "UF": str(row["uf"]),
        "Ano": int(row["ano"]),
        "Trimestre": str(row["trimestre"]),
        "ValorDespesas": float(row["valordespesas"]),
        # soma de todos os trimestres
        "TotalDespesas": float(df["valordespesas"].sum()),
        "CNPJ": str(row["cnpj"]),
        "Modalidade": str(row["modalidade"]),
    }


@app.get("/api/operadoras/{registro_ans}/despesas")
def historico_despesas(registro_ans: str):
    df = normalizacao[normalizacao["registroans"].astype(str) == registro_ans]
    if df.empty:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")

    return [
        {
            "Ano": int(row["ano"]),
            "Trimestre": str(row["trimestre"]),
            "ValorDespesas": float(row["valordespesas"]),
            # soma total da operadora
            "TotalDespesas": float(df["valordespesas"].sum()),
            "MediaTrimestral": float(row["mediatrimestral"]),
            "DesvioPadrao": float(row["desviopadrao"]),
        }
        for _, row in df.iterrows()
    ]


@app.get("/api/estatisticas")
def estatisticas():
    if normalizacao.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")

    total = float(normalizacao["totaldespesas"].sum())
    media = float(normalizacao["totaldespesas"].mean())

    top_5 = normalizacao.groupby("registroans").sum(
        "totaldespesas").sort_values("totaldespesas", ascending=False).head(5)
    top_5_list = [
        {
            "RegistroANS": int(row.name),
            "RazaoSocial": str(normalizacao[normalizacao["registroans"] == row.name].iloc[0]["razaosocial"]),
            "UF": str(normalizacao[normalizacao["registroans"] == row.name].iloc[0]["uf"]),
            "TotalDespesas": float(row["totaldespesas"]),
        }
        for _, row in top_5.iterrows()
    ]

    return {
        "total_despesas": total,
        "media_despesas": media,
        "top_5_operadoras": top_5_list,
    }


@app.get("/api/operadoras/todos")
def listar_todas_operadoras():
    if normalizacao.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")

    return [
        {
            "RegistroANS": int(row["registroans"]),
            "RazaoSocial": str(row["razaosocial"]),
            "UF": str(row["uf"]),
            "Ano": int(row["ano"]),
            "Trimestre": str(row["trimestre"]),
            "ValorDespesas": float(row["valordespesas"])
        }
        for _, row in normalizacao.iterrows()
    ]
