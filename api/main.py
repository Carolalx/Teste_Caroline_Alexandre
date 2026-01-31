from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import glob
from pathlib import Path
from typing import List, Dict

app = FastAPI(title="API de Operadoras")

# --- CONFIGURAÇÃO DE CORS --- #
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

# --- CAMINHO CSV com possibilidade de sufixo ao exportar do DBeaver--- #
DATA_DIR = Path(__file__).parent.parent / "data"
resultado_despesas_pattern = str(DATA_DIR / "resultado_despesas*.csv")

resultado_despesas = pd.DataFrame()

# --- LEITURA E PROCESSAMENTO DO CSV --- #
try:
    resultado_despesas_files = glob.glob(resultado_despesas_pattern)

    if not resultado_despesas_files:
        print(f"AVISO: Nenhum CSV encontrado em {DATA_DIR}")
    else:
        RESULTADO_DESPESAS_PATH = resultado_despesas_files[0]
        print(f"Carregando arquivo: {RESULTADO_DESPESAS_PATH}")

        resultado_despesas = pd.read_csv(
            RESULTADO_DESPESAS_PATH, encoding="utf-8", sep=None, engine='python')

        resultado_despesas.columns = [c.lower().strip()
                                      for c in resultado_despesas.columns]

        num_cols = ["valordespesas", "totaldespesas",
                    "mediatrimestral", "desviopadrao", "registroans", "ano"]
        for col in num_cols:
            if col in resultado_despesas.columns:
                resultado_despesas[col] = (
                    resultado_despesas[col].astype(str)
                    .str.replace('.', '', regex=False)
                    .str.replace(',', '.', regex=False)
                    .str.strip()
                )
                resultado_despesas[col] = pd.to_numeric(
                    resultado_despesas[col], errors='coerce').fillna(0)

        resultado_despesas = resultado_despesas.dropna(
            subset=["registroans", "razaosocial"])

        print(f"Sucesso! {len(resultado_despesas)} linhas carregadas.")

except Exception as e:
    print(f"ERRO CRÍTICO NO BACKEND: {e}")


# --- ROTAS OPERADORAS --- #

@app.get("/api/operadoras")
def listar_operadoras(page: int = 1, limit: int = 50, q: str = None):
    if resultado_despesas.empty:
        return {"data": [], "total": 0}

    df_result = resultado_despesas.copy()

    # --- Verificação do valor de 'q' --- #
    print(f"Termo de busca: {q}")

    # --- BUSCA GLOBAL NO DATAFRAME --- #
    if q:
        q = q.lower()  # Normaliza o termo de busca para minúsculo
        print(f"Busca com o termo: {q}")  # Exibe o termo de busca no terminal

        # Filtro por Razão Social, Registro ANS ou CNPJ (em minúsculas para busca insensível a maiúsculas)
        mask = (
            df_result["razaosocial"].astype(str).str.lower().str.contains(q) |
            df_result["registroans"].astype(str).str.contains(q) |
            df_result["cnpj"].astype(str).str.contains(
                q)  # Adiciona a busca pelo CNPJ
        )
        df_result = df_result[mask]

    total_filtrado = len(df_result)

    # --- PAGINAÇÃO DOS RESULTADOS FILTRADOS --- #
    start = (page - 1) * limit
    end = start + limit
    df_page = df_result.iloc[start:end]

    data = []
    for _, row in df_page.iterrows():
        data.append({
            "RegistroANS": int(row["registroans"]),
            "RazaoSocial": str(row["razaosocial"]),
            "CNPJ": str(row["cnpj"]),  # Incluindo CNPJ nos dados retornados
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


# --- ROTAS DE ESTATÍSTICAS --- #

# 1. Operadoras com Maior Crescimento Percentual
@app.get("/api/estatisticas/crescimento")
def obter_crescimento_percentual():
    if resultado_despesas.empty:
        return []

    # 1. Criar a chave temporal correta primeiro
    df = resultado_despesas.copy()
    df['periodo'] = df['ano'].astype(str) + '-' + df['trimestre'].astype(str)

    # 2. Agrupar para achar o min/max do PERÍODO, não só do ano
    primeiro_ultimo = df.groupby(['registroans', 'razaosocial']).agg(
        primeiro_periodo=('periodo', 'min'),
        ultimo_periodo=('periodo', 'max')
    ).reset_index()

    # 3. Merge para buscar os valores iniciais
    primeiro_ultimo = primeiro_ultimo.merge(
        df[['registroans', 'periodo', 'valordespesas']],
        left_on=['registroans', 'primeiro_periodo'],
        right_on=['registroans', 'periodo'],
        how='left'
    ).rename(columns={'valordespesas': 'valordespesas_inicial'}).drop(columns=['periodo'])

    # 4. Merge para buscar os valores finais
    primeiro_ultimo = primeiro_ultimo.merge(
        df[['registroans', 'periodo', 'valordespesas']],
        left_on=['registroans', 'ultimo_periodo'],
        right_on=['registroans', 'periodo'],
        how='left'
    ).rename(columns={'valordespesas': 'valordespesas_final'}).drop(columns=['periodo'])

    # 5. Cálculo com a mesma lógica do SQL (* 100)
    # tratando a divisão por zero
    mask = primeiro_ultimo['valordespesas_inicial'] != 0
    primeiro_ultimo['crescimento_percentual'] = 0.0

    primeiro_ultimo.loc[mask, 'crescimento_percentual'] = (
        (primeiro_ultimo['valordespesas_final'] - primeiro_ultimo['valordespesas_inicial']) /
        primeiro_ultimo['valordespesas_inicial']
    ) * 100

    # Ordenação e limpeza
    top_5 = primeiro_ultimo.sort_values(
        by='crescimento_percentual', ascending=False).head(5)
    return top_5.to_dict(orient='records')


# 2. Distribuição de Despesas por UF
@app.get("/api/estatisticas/despesas_uf")
def obter_despesas_por_uf():
    if resultado_despesas.empty:
        return []

    despesas_por_uf = resultado_despesas.groupby(['uf']).agg(
        total_despesas_uf=('totaldespesas', 'sum'),
        media_despesas_por_operadora=('totaldespesas', 'mean')
    ).reset_index()

    # Seleciona os 5 estados com maiores despesas totais
    top_5_uf = despesas_por_uf.sort_values(
        by='total_despesas_uf', ascending=False).head(5)

    return top_5_uf.to_dict(orient='records')


# 3. Operadoras Acima da Média
@app.get("/api/estatisticas/acima_media")
def obter_operadoras_acima_media():
    if resultado_despesas.empty:
        return []

    # 1. Calcular a média geral das despesas
    media_geral = resultado_despesas['valordespesas'].mean()

    # 2. Criar a coluna 'acima_media' que indica se as despesas estão acima da média
    operadoras_acima_media = resultado_despesas.copy()
    operadoras_acima_media['acima_media'] = operadoras_acima_media['valordespesas'] > media_geral

    # 3. Contagem dos trimestres acima da média e soma das despesas
    operadoras_acima_media = operadoras_acima_media.groupby(['registroans', 'razaosocial']).agg(
        trimestres_acima_media=('acima_media', 'sum'),
        total_despesas_acima_media=('valordespesas', 'sum')
    ).reset_index()

    # 4. Filtrar as operadoras que têm mais de 2 trimestres acima da média
    operadoras_acima_media = operadoras_acima_media[operadoras_acima_media['trimestres_acima_media'] >= 2]

    # 5. Simular o RANK() (classificação) com base na lógica do SQL
    operadoras_acima_media['ranking'] = operadoras_acima_media['trimestres_acima_media'] * \
        1000 + operadoras_acima_media['total_despesas_acima_media']

    # 6. Ordenar pela classificação para obter o ranking desejado
    operadoras_acima_media = operadoras_acima_media.sort_values(
        by=['trimestres_acima_media', 'total_despesas_acima_media'],
        ascending=[False, False]
    ).reset_index(drop=True)

    # 7. Retornar apenas as colunas desejadas
    return operadoras_acima_media[['registroans', 'razaosocial', 'trimestres_acima_media', 'total_despesas_acima_media', 'ranking']].to_dict(orient='records')
