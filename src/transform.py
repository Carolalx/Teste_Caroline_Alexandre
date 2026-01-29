import pandas as pd
import requests
import io
import os
import zipfile
import utils  # Se estiver na mesma pasta src, importe apenas 'utils'
import logging
import urllib3

# --- SILENCIADOR DE AVISOS ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.captureWarnings(True)

# Configurações de Caminho (Relativos à pasta src)
ARQUIVO_ETAPA1 = "../data/consolidado_despesas.csv"
URL_CADASTRO = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
ARQUIVO_SAIDA_AGREGADO = "../data/despesas_agregadas.csv"
ARQUIVO_CADASTRO_LOCAL = "../data/tabela_cadastro_operadoras.csv"

def processar_transform():
    print("🚀 Iniciando Transformação e Enriquecimento...")
    
    # 1. Verificação do arquivo de entrada
    if not os.path.exists(ARQUIVO_ETAPA1):
        print(f"❌ Erro: {ARQUIVO_ETAPA1} não encontrado. Execute o main.py primeiro.")
        return
    
    df_fin = pd.read_csv(ARQUIVO_ETAPA1)
    
    # 2. Download e Preparação do Cadastro
    try:
        print("🌐 Baixando cadastro de operadoras da ANS...")
        resp = requests.get(URL_CADASTRO, timeout=30, verify=False)
        df_cadop = pd.read_csv(io.BytesIO(resp.content), sep=';', encoding='latin1', on_bad_lines='skip')
        
        # Limpeza básica de nomes
        df_cadop.columns = df_cadop.columns.str.strip().str.upper()
        df_cadop = df_cadop.rename(columns={
            'REGISTRO_OPERADORA': 'RegistroANS',
            'RAZÃO_SOCIAL': 'RazaoSocial',
            'RAZAO_SOCIAL': 'RazaoSocial',
            'UF': 'UF'
        })
        
        # Validação de CNPJ usando o Utils (Etapa 2.1 do edital)
        if 'CNPJ' in df_cadop.columns:
            print("🔍 Validando CNPJs...")
            df_cadop['CNPJ_VALIDO'] = df_cadop['CNPJ'].apply(utils.validar_cnpj)

    except Exception as e:
        print(f"❌ Erro no processamento do cadastro: {e}")
        return

    # 3. Merge e Agregação (Itens 2.2 e 2.3 do edital)
    print("📊 Cruzando dados e calculando estatísticas...")
    df_fin['RegistroANS'] = pd.to_numeric(df_fin['RegistroANS'], errors='coerce')
    df_cadop['RegistroANS'] = pd.to_numeric(df_cadop['RegistroANS'], errors='coerce')

    # Merge para trazer RazaoSocial e UF para o financeiro
    df_res = pd.merge(
        df_cadop[['RegistroANS', 'RazaoSocial', 'UF']], 
        df_fin.drop(columns=['RazaoSocial'], errors='ignore'), 
        on='RegistroANS', 
        how='inner'
    )
    
    # Agregação solicitada
    agregado = df_res.groupby(['RegistroANS', 'RazaoSocial', 'UF'])['ValorDespesas'].agg(
        TotalDespesas='sum', 
        MediaTrimestral='mean', 
        DesvioPadrao='std'
    ).reset_index().fillna(0)

    # 4. Salvamento dos arquivos locais
    print("💾 Salvando arquivos processados em /data...")
    os.makedirs("../data", exist_ok=True) # Garante que a pasta existe
    agregado.to_csv(ARQUIVO_SAIDA_AGREGADO, index=False, encoding='utf-8')
    df_cadop.to_csv(ARQUIVO_CADASTRO_LOCAL, index=False, encoding='utf-8')
    
    # 5. Geração do ZIP final na raiz do projeto
    print("📦 Gerando pacote ZIP final...")
    try:
        with zipfile.ZipFile("../Teste_Caroline_Alexandre.zip", 'w', zipfile.ZIP_DEFLATED) as z:
            # O primeiro argumento é o arquivo no disco, o segundo é o nome dentro do zip
            z.write(ARQUIVO_SAIDA_AGREGADO, arcname="despesas_agregadas.csv")
            z.write(ARQUIVO_ETAPA1, arcname="consolidado_despesas.csv")
            z.write(ARQUIVO_CADASTRO_LOCAL, arcname="tabela_cadastro_operadoras.csv")
        print("✨ Sucesso! O arquivo 'Teste_Caroline_Alexandre.zip' foi criado na raiz.")
    except Exception as e:
        print(f"⚠️ Erro ao criar ZIP: {e}")

# Ponto de entrada do script
if __name__ == "__main__":
    processar_transform()