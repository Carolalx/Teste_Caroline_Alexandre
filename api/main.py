from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import glob
from pathlib import Path

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

# Padrão do nome do arquivo, considerando que pode haver um sufixo
resultado_despesas_pattern = str(DATA_DIR / "resultado_despesas*.csv")

# --- Carrega CSV --- #
resultado_despesas = pd.DataFrame()

try:
    # Procurar o arquivo que corresponde ao padrão
    resultado_despesas_files = glob.glob(resultado_despesas_pattern)

    # Verificar se encontrou o arquivo
    if not resultado_despesas_files:
        raise FileNotFoundError(f"CSV não encontrado no diretório: {DATA_DIR}")

    # Pega o primeiro arquivo encontrado
    RESULTADO_DESPESAS_PATH = resultado_despesas_files[0]
    print(f"Arquivo encontrado: {RESULTADO_DESPESAS_PATH}")

    # Carrega o CSV encontrado
    resultado_despesas = pd.read_csv(RESULTADO_DESPESAS_PATH, encoding="utf-8")

    # --- Colunas obrigatórias --- #
    required_cols = [
        "registroans", "cnpj", "razaosocial", "modalidade", "uf", "trimestre", "ano",
        "valordespesas", "totaldespesas", "mediatrimestral", "desviopadrao"
    ]

    for col in required_cols:
        if col not in resultado_despesas.columns:
            raise ValueError(f"CSV não contém coluna obrigatória: {col}")

    # --- Converter colunas numéricas corretamente --- #
    num_cols = ["valordespesas", "totaldespesas",
                "mediatrimestral", "desviopadrao"]
    for col in num_cols:
        resultado_despesas[col] = (
            resultado_despesas[col].astype(str)
            .str.replace(",", "")  # remove vírgulas de milhar
            .str.replace(" ", "")  # remove espaços
            .replace({"": "0", "nan": "0"}, regex=True)
            .astype(float)
        )

    # Remove linhas com valor 0 em valordespesas
    resultado_despesas = resultado_despesas[resultado_despesas["valordespesas"] != 0]

    print(
        f"CSV carregado com sucesso! Total de linhas: {len(resultado_despesas)}")

except Exception as e:
    print("Erro ao carregar CSV:", e)


# --- Rotas --- #

@app.get("/api/operadoras")
def listar_operadoras(page: int = 1, limit: int = 50):
    if resultado_despesas.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")

    start = (page - 1) * limit
    end = start + limit
    df_page = resultado_despesas.iloc[start:end]

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
        "total": len(resultado_despesas),
        "data": data
    }


@app.get("/api/operadoras/{registro_ans}")
def detalhe_operadora(registro_ans: str):
    df = resultado_despesas[resultado_despesas["registroans"].astype(
        str) == registro_ans]
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
    df = resultado_despesas[resultado_despesas["registroans"].astype(
        str) == registro_ans]
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
    if resultado_despesas.empty:
        raise HTTPException(status_code=500, detail="Dados não carregados")

    try:
        # Verificando a coluna 'totaldespesas' antes da conversão:
        print("Verificando a coluna 'totaldespesas' antes da conversão:")
        # Exibir as primeiras linhas para diagnóstico
        print(resultado_despesas['totaldespesas'].head())

        # Garantir que a coluna 'totaldespesas' seja numérica, forçando conversão
        resultado_despesas['totaldespesas'] = pd.to_numeric(
            resultado_despesas['totaldespesas'], errors='coerce')

        # Verificar se existem valores NaN após conversão
        if resultado_despesas['totaldespesas'].isna().any():
            raise ValueError(
                "Existem valores NaN na coluna 'totaldespesas' após a conversão.")

        # Verificando os dados após a conversão:
        print("Dados após conversão da coluna 'totaldespesas':")
        # Exibir novamente as primeiras linhas
        print(resultado_despesas['totaldespesas'].head())

        # Agora podemos calcular as estatísticas corretamente
        total = resultado_despesas["totaldespesas"].sum()
        media = resultado_despesas["totaldespesas"].mean()

        print(f"Total de despesas: {total}")
        print(f"Média de despesas: {media}")

        # Top 5 operadoras por 'totaldespesas'
        top_5 = resultado_despesas.groupby("registroans").agg({"totaldespesas": "sum"}).sort_values(
            "totaldespesas", ascending=False).head(5)

        # Prepare o retorno com as top 5 operadoras
        top_5_list = [
            {
                "RegistroANS": int(row.name),
                "RazaoSocial": str(resultado_despesas[resultado_despesas["registroans"] == row.name].iloc[0]["razaosocial"]),
                "UF": str(resultado_despesas[resultado_despesas["registroans"] == row.name].iloc[0]["uf"]),
                "TotalDespesas": float(row["totaldespesas"]),
            }
            for _, row in top_5.iterrows()
        ]

        return {
            "total_despesas": total,
            "media_despesas": media,
            "top_5_operadoras": top_5_list,
        }

    except ValueError as ve:
        # Erro ao detectar NaNs ou falhas na conversão
        print("Erro de valor na conversão de dados:", ve)
        raise HTTPException(
            status_code=500, detail=f"Erro de valor na conversão de dados: {str(ve)}")

    except Exception as e:
        # Erro geral
        print("Erro durante o cálculo das estatísticas:", str(e))
        raise HTTPException(
            status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")


@app.get("/api/operadoras/todos")
def listar_todas_operadoras():
    if resultado_despesas.empty:
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
        for _, row in resultado_despesas.iterrows()
    ]
