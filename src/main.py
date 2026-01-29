import os
import requests
import zipfile
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import io
import re
import src.utils as utils  # Importando suas funções auxiliares

# Configurações de Caminho
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
# Salvando na pasta data que está um nível acima
OUTPUT_CSV = "../data/consolidado_despesas.csv"
TRIMESTRES_ALVO = 3

def get_last_3_quarters():
    headers = {'User-Agent': 'Mozilla/5.0'}
    all_quarter_links = []
    try:
        res_anos = requests.get(BASE_URL, headers=headers, timeout=15)
        soup_anos = BeautifulSoup(res_anos.text, 'html.parser')
        anos = sorted([a['href'] for a in soup_anos.find_all('a') if re.match(r'20\d{2}/?', a['href'])], reverse=True)

        for ano in anos:
            url_ano = urljoin(BASE_URL, ano)
            res_q = requests.get(url_ano, headers=headers, timeout=15)
            soup_q = BeautifulSoup(res_q.text, 'html.parser')
            zips = [urljoin(url_ano, a['href']) for a in soup_q.find_all('a') if a['href'].lower().endswith('.zip')]
            zips.sort(reverse=True)
            for z in zips:
                all_quarter_links.append(z)
                if len(all_quarter_links) == TRIMESTRES_ALVO:
                    return all_quarter_links
    except Exception as e:
        print(f"Erro ao buscar diretórios: {e}")
    return all_quarter_links

def download_and_process():
    targets = get_last_3_quarters()
    if not targets: return

    consolidated_data = []
    for zip_url in targets:
        zip_name = zip_url.split('/')[-1]
        print(f"📦 Processando: {zip_name}")
        try:
            r = requests.get(zip_url, timeout=120)
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                for file_name in z.namelist():
                    if file_name.lower().endswith('.csv'):
                        with z.open(file_name) as f:
                            # Uso do utils para leitura flexível e normalização
                            df = pd.read_csv(f, sep=None, engine='python', encoding='latin1', on_bad_lines='skip')
                            df = utils.normalizar_colunas(df)
                            df = utils.tratar_inconsistencias(df)
                            
                            df['Trimestre'] = re.findall(r'(\dT)', zip_name)[0] if re.search(r'\dT', zip_name) else "N/A"
                            df['Ano'] = re.findall(r'(20\d{2})', zip_name)[0] if re.search(r'20\d{2}', zip_name) else "N/A"
                            consolidated_data.append(df)
        except Exception as e:
            print(f"⚠️ Erro no ZIP {zip_name}: {e}")

    if consolidated_data:
        final_df = pd.concat(consolidated_data, ignore_index=True)
        # Garantir colunas do projeto
        colunas = ['CNPJ', 'RegistroANS', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
        for col in colunas:
            if col not in final_df.columns: final_df[col] = ""
        
        # Criar pasta data se não existir
        os.makedirs("../data", exist_ok=True)
        final_df[colunas].to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"✨ SUCESSO! '{OUTPUT_CSV}' gerado.")

if __name__ == "__main__":
    download_and_process()