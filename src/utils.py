# src/utils.py
import pandas as pd
import re
from collections import Counter


def limpar_cnpj(cnpj):
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r'\D', '', str(cnpj)).zfill(14)


def validar_cnpj(cnpj):
    """Valida CNPJ, verificando formato e dígitos verificadores."""
    cnpj = limpar_cnpj(cnpj)

    if len(cnpj) != 14:
        return False  # CNPJ deve ter 14 dígitos

    # Validação de dígitos verificadores do CNPJ
    def calcular_dv(cnpj, pesos):
        """Calcula dígitos verificadores do CNPJ."""
        soma = sum(int(cnpj[i]) * pesos[i] for i in range(len(pesos)))
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    # Pesos para os dois primeiros dígitos verificadores
    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    dv1 = calcular_dv(cnpj, pesos_1)
    dv2 = calcular_dv(cnpj, pesos_2)

    return cnpj[-2:] == f"{dv1}{dv2}"


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
    df = df.rename(
        columns={k: v for k, v in mapeamento.items() if k in df.columns})
    return df


def tratar_inconsistencias(df):
    """Aplica regras de negócio focando em REG_ANS, Valores e exclusão de linhas com dados inválidos."""

    # 1. Garantir CNPJ se ele existir (pode vir como CNPJ ou CNPJ_OPERADORA)
    if 'CNPJ' in df.columns:
        df['CNPJ'] = df['CNPJ'].apply(limpar_cnpj)
        df['CNPJ_STATUS'] = df['CNPJ'].apply(
            lambda x: 'Inconsistente' if not validar_cnpj(x) else 'Válido')

    # 2. Garantir REG_ANS como string/inteiro limpo
    if 'RegistroANS' in df.columns:
        df['RegistroANS'] = pd.to_numeric(
            df['RegistroANS'], errors='coerce').fillna(0).astype(int)

    # 3. Tratar valores (Despesas)
    if 'ValorDespesas' in df.columns:
        if df['ValorDespesas'].dtype == 'object':
            df['ValorDespesas'] = df['ValorDespesas'].str.replace(',', '.')
        df['ValorDespesas'] = pd.to_numeric(
            df['ValorDespesas'], errors='coerce').fillna(0)

        # Substituindo valores negativos por 0
        df['ValorDespesas'] = df['ValorDespesas'].apply(
            lambda x: 0 if x < 0 else x)

    if 'VL_SALDO_INICIAL' in df.columns:
        if df['VL_SALDO_INICIAL'].dtype == 'object':
            df['VL_SALDO_INICIAL'] = df['VL_SALDO_INICIAL'].str.replace(
                ',', '.')
        df['VL_SALDO_INICIAL'] = pd.to_numeric(
            df['VL_SALDO_INICIAL'], errors='coerce').fillna(0)

        # Substituindo valores negativos por 0
        df['VL_SALDO_INICIAL'] = df['VL_SALDO_INICIAL'].apply(
            lambda x: 0 if x < 0 else x)

    # 4. Excluir linhas onde VL_SALDO_INICIAL e ValorDespesas são ambos 0
    if 'VL_SALDO_INICIAL' in df.columns and 'ValorDespesas' in df.columns:
        df = df[~((df['VL_SALDO_INICIAL'] == 0) & (df['ValorDespesas'] == 0))]

    # 5. Verificar CNPJs duplicados com diferentes Razões Sociais
    if 'CNPJ' in df.columns and 'RazaoSocial' in df.columns:
        duplicated_cnpjs = df[df.duplicated(subset='CNPJ', keep=False)]

        for cnpj, group in duplicated_cnpjs.groupby('CNPJ'):
            razoes_sociais = group['RazaoSocial'].unique()
            if len(razoes_sociais) > 1:
                print(
                    f"⚠️ Inconsistência detectada no CNPJ {cnpj}: Razões sociais diferentes.")
                # Escolher a razão social mais recente, com base na Data
                grupo_com_data = group.dropna(
                    subset=['Data']).sort_values('Data', ascending=False)
                razao_social_recente = grupo_com_data.iloc[0]['RazaoSocial']
                # Corrige para a mais recente
                df.loc[df['CNPJ'] == cnpj, 'RazaoSocial'] = razao_social_recente

    # 6. Validar Trimestres
    if 'Trimestre' in df.columns:
        df['Trimestre'] = df['Trimestre'].apply(
            lambda x: 'Inconsistente' if not re.match(r'\dT\d{4}', str(x)) else x)

    # 7. Validar Anos
    if 'Ano' in df.columns:
        df['Ano'] = df['Ano'].apply(
            lambda x: 'Inconsistente' if not re.match(r'\d{4}', str(x)) else x)

    # 8. Preencher razão social vazia com "Razão Social Não Informada"
    if 'RazaoSocial' in df.columns:
        df['RazaoSocial'] = df['RazaoSocial'].apply(
            lambda x: x if pd.notnull(x) and x != '' else 'Razão Social Não Informada')

    return df
