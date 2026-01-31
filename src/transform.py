import pandas as pd
import requests
import io
import os
import zipfile
import utils
import logging
import urllib3

# --- SILENCIADOR DE AVISOS ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.captureWarnings(True)

# Configurações de Caminho (Relativos à pasta src)
ARQUIVO_ETAPA1 = "data/consolidado_despesas.csv"
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
ARQUIVO_SAIDA_AGREGADO = "data/despesas_agregadas.csv"
ARQUIVO_CADASTRO_LIMPO = "data/tabela_cadastro_operadoras_limpo.csv"
PASTA_ZIP = "data"  # Caminho da pasta onde os arquivos CSV e o ZIP são salvos


def processar_transform():
    print("🚀 Iniciando Transformação e Enriquecimento...")

    # 1. Verificação do arquivo de entrada
    if not os.path.exists(ARQUIVO_ETAPA1):
        print(
            f"❌ Erro: {ARQUIVO_ETAPA1} não encontrado. Execute o main.py primeiro.")
        return

    # Carregar o CSV consolidado
    df_fin = pd.read_csv(ARQUIVO_ETAPA1)

    # Tratar duplicidade de consolidado_despesas (df_fin)
    print("🔍 Tratando duplicidades no arquivo consolidado_despesas...")
    df_fin['ValorDespesas'] = pd.to_numeric(
        df_fin['ValorDespesas'], errors='coerce').fillna(0)

    # Remover duplicatas baseadas em RegistroANS, Ano, Trimestre e ValorDespesas
    df_fin = df_fin.drop_duplicates(
        subset=['RegistroANS', 'Trimestre', 'Ano', 'ValorDespesas']
    )

    # 2. Download e Preparação do Cadastro
    try:
        print("🌐 Baixando cadastro de operadoras da ANS...")
        resp = requests.get(URL_CADASTRO, timeout=30, verify=False)
        df_cadop = pd.read_csv(io.BytesIO(resp.content),
                               sep=';', encoding='latin1', on_bad_lines='skip')

        # Limpeza básica de nomes
        df_cadop.columns = df_cadop.columns.str.strip().str.upper()

        # Renomeando colunas para garantir que coincidam com as esperadas
        df_cadop = df_cadop.rename(columns={
            'REGISTRO_OPERADORA': 'RegistroANS',
            'RAZÃO_SOCIAL': 'RazaoSocial',
            'RAZAO_SOCIAL': 'RazaoSocial',
            'UF': 'UF',
            'DATA_REGISTRO_ANS': 'DataRegistroANS',  # Data de registro ANS
            'MODALIDADE': 'Modalidade'
        })

        # Ordenando pelo CNPJ e, em seguida, pela Data de Registro ANS (para manter a RazaoSocial mais recente)
        df_cadop['DataRegistroANS'] = pd.to_datetime(
            df_cadop['DataRegistroANS'], errors='coerce')
        df_cadop = df_cadop.sort_values(
            by=['CNPJ', 'DataRegistroANS'], ascending=[True, False])

        # Remover duplicatas de CNPJ, mantendo a RazaoSocial mais recente
        df_cadop = df_cadop.drop_duplicates(subset=['CNPJ'], keep='first')

        # Filtrando apenas as colunas necessárias
        df_cadop = df_cadop[['RegistroANS', 'CNPJ',
                             'RazaoSocial', 'Modalidade', 'UF']]

        # Salvando o arquivo tratado
        df_cadop.to_csv(ARQUIVO_CADASTRO_LIMPO, index=False, encoding='utf-8')

    except Exception as e:
        print(f"❌ Erro no processamento do cadastro: {e}")
        return

    # 3. Merge entre os dados consolidados e o cadastro tratado
    print("📊 Cruzando dados e calculando estatísticas...")

    df_res = pd.merge(
        df_cadop[['RegistroANS', 'RazaoSocial', 'UF']],
        df_fin.drop(columns=['RazaoSocial'], errors='ignore'),
        on='RegistroANS',
        how='left'  # Mantém todos os dados de df_fin e adiciona informações do cadastro
    )

    # Agregação solicitada
    agregado = df_res.groupby(['RegistroANS', 'RazaoSocial', 'UF'])['ValorDespesas'].agg(
        TotalDespesas='sum',
        MediaTrimestral='mean',
        DesvioPadrao='std'
    ).reset_index().fillna(0)

    # Excluir linhas onde as colunas de valores (TotalDespesas, MediaTrimestral, DesvioPadrao) são 0
    agregado = agregado[
        (agregado['TotalDespesas'] != 0) |
        (agregado['MediaTrimestral'] != 0) |
        (agregado['DesvioPadrao'] != 0)
    ]

    # 4. Salvamento dos arquivos locais
    agregado.to_csv(ARQUIVO_SAIDA_AGREGADO, index=False, encoding='utf-8')

    # 5. Geração do ZIP final na pasta 'data'
    print("📦 Gerando pacote ZIP final...")

    try:
        # Caminho completo para o arquivo ZIP na pasta 'data'
        zip_path = os.path.join(PASTA_ZIP, "Teste_Caroline_Alexandre.zip")

        # Verificação se os arquivos existem antes de criar o ZIP
        if not os.path.exists(ARQUIVO_SAIDA_AGREGADO):
            raise FileNotFoundError(
                f"O arquivo {ARQUIVO_SAIDA_AGREGADO} não foi encontrado.")
        if not os.path.exists(ARQUIVO_ETAPA1):
            raise FileNotFoundError(
                f"O arquivo {ARQUIVO_ETAPA1} não foi encontrado.")
        if not os.path.exists(ARQUIVO_CADASTRO_LIMPO):
            raise FileNotFoundError(
                f"O arquivo {ARQUIVO_CADASTRO_LIMPO} não foi encontrado.")

        # Criação do arquivo ZIP
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
            z.write(ARQUIVO_SAIDA_AGREGADO, arcname="despesas_agregadas.csv")
            z.write(ARQUIVO_ETAPA1, arcname="consolidado_despesas.csv")
            z.write(ARQUIVO_CADASTRO_LIMPO,
                    arcname="tabela_cadastro_operadoras_limpo.csv")

        print(f"✨ Sucesso! O arquivo ZIP foi criado em {zip_path}.")
    except Exception as e:
        print(f"⚠️ Erro ao criar o ZIP: {e}")


# Ponto de entrada do script
if __name__ == "__main__":
    processar_transform()
