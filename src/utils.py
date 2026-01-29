import pandas as pd
import re

def limpar_cnpj(cnpj):
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r'\D', '', str(cnpj)).zfill(14)

def normalizar_colunas(df):
    """Mapeia colunas variadas da ANS para o padrão do projeto."""
    mapeamento = {
        'CNPJ_OPERADORA': 'CNPJ',
        'REG_ANS': 'RegistroANS',
        'RAZAO_SOCIAL': 'RazaoSocial',
        'NM_RAZAO_SOCIAL': 'RazaoSocial',
        'VL_SALDO_FINAL': 'ValorDespesas',
        'VALOR': 'ValorDespesas',
        'DT_REGISTRO': 'Data'
    }
    # Renomeia apenas as colunas que existirem no DF atual
    df = df.rename(columns={k: v for k, v in mapeamento.items() if k in df.columns})
    return df

def tratar_inconsistencias(df):
    """Aplica regras de negócio focando em REG_ANS e Valores."""
    # 1. Garantir CNPJ se ele existir (pode vir como CNPJ ou CNPJ_OPERADORA)
    if 'CNPJ' in df.columns:
        df['CNPJ'] = df['CNPJ'].apply(limpar_cnpj)
    
    # 2. Garantir REG_ANS como string/inteiro limpo
    if 'RegistroANS' in df.columns:
        df['RegistroANS'] = pd.to_numeric(df['RegistroANS'], errors='coerce').fillna(0).astype(int)
    
    # 3. Tratar valores (Despesas)
    if 'ValorDespesas' in df.columns:
        # Em alguns arquivos da ANS, o valor vem com vírgula como decimal
        if df['ValorDespesas'].dtype == 'object':
            df['ValorDespesas'] = df['ValorDespesas'].str.replace(',', '.')
        df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce').fillna(0)
    
    return df

def validar_cnpj(cnpj):
    """Valida o formato e os dígitos verificadores do CNPJ."""
    cnpj = str(cnpj).zfill(14)
    if len(cnpj) != 14 or cnpj in [s * 14 for s in "0123456789"]:
        return False

    def calcular_digito(cnpj, pesos):
        soma = sum(int(a) * b for a, b in zip(cnpj, pesos))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    
    if int(cnpj[12]) != calcular_digito(cnpj[:12], pesos1):
        return False
    if int(cnpj[13]) != calcular_digito(cnpj[:13], pesos2):
        return False
    return True
