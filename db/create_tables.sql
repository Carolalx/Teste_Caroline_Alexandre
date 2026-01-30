-- 1. Limpeza de ambiente
DROP TABLE IF EXISTS consolidado_despesas CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
DROP TABLE IF EXISTS operadoras_cadastro CASCADE;
DROP TABLE IF EXISTS normalizacao CASCADE;


-- 2. Criação da Tabela de Cadastro (Dimensão)
CREATE TABLE operadoras_cadastro (
    RegistroANS INTEGER PRIMARY KEY,  -- Chave primária
    CNPJ VARCHAR(20),
    RazaoSocial TEXT,
    Nome_Fantasia TEXT,
    Modalidade TEXT,
    Logradouro TEXT,
    Numero TEXT,
    Complemento TEXT,
    Bairro TEXT,
    Cidade TEXT,
    UF CHAR(2),
    CEP TEXT,
    DDD TEXT,
    Telefone TEXT,
    Endereco_Eletronico TEXT,
    Representante TEXT,
    Cargo_Representante TEXT,
    Regiao_de_Comercializacao TEXT,
    Data_Registro_ANS DATE
);


CREATE TABLE consolidado_despesas (
    CNPJ VARCHAR(20),
    RegistroANS INTEGER,
    Trimestre VARCHAR(5),
    Ano INTEGER,
    ValorDespesas NUMERIC(18,2)
);


CREATE TABLE despesas_agregadas (
    RegistroANS INTEGER,
    RazaoSocial TEXT,
    UF CHAR(2),
    TotalDespesas NUMERIC(18,2),
    MediaTrimestral NUMERIC(18,2),
    DesvioPadrao NUMERIC(18,2)
);


CREATE TABLE normalizacao (
    id SERIAL PRIMARY KEY,  -- Chave única artificial
    RegistroANS INTEGER,
    Ano INTEGER,
    Trimestre VARCHAR(5),

    -- Dados de cadastro
    CNPJ VARCHAR(20),
    RazaoSocial TEXT,
    Modalidade TEXT,
    UF CHAR(2),
    Data_Registro_ANS DATE,

    -- Despesas
    ValorDespesas NUMERIC(18,2),

    -- Estatísticas agregadas
    TotalDespesas NUMERIC(18,2),
    MediaTrimestral NUMERIC(18,2),
    DesvioPadrao NUMERIC(18,2)
);

WITH consolidado_agrupado AS (
    SELECT 
        RegistroANS,
        Ano,
        Trimestre,
        SUM(ValorDespesas) AS ValorDespesas  -- Soma as duplicidades
    FROM consolidado_despesas
    GROUP BY RegistroANS, Ano, Trimestre
)
INSERT INTO normalizacao (
    RegistroANS, Ano, Trimestre,
    CNPJ, RazaoSocial, Modalidade, UF, Data_Registro_ANS,
    ValorDespesas, TotalDespesas, MediaTrimestral, DesvioPadrao
)
SELECT 
    ca.RegistroANS,
    ca.Ano,
    ca.Trimestre,
    oc.CNPJ,
    oc.RazaoSocial,
    oc.Modalidade,
    oc.UF,
    oc.Data_Registro_ANS,
    ca.ValorDespesas,
    da.TotalDespesas,
    da.MediaTrimestral,
    da.DesvioPadrao
FROM consolidado_agrupado ca
JOIN operadoras_cadastro oc
    ON ca.RegistroANS = oc.RegistroANS
LEFT JOIN despesas_agregadas da
    ON ca.RegistroANS = da.RegistroANS
    AND oc.UF = da.UF
WHERE ca.ValorDespesas <> 0;  -- Remove as despesas com valor 0


