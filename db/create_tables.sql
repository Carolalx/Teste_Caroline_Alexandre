-- 1. Limpeza de ambiente
DROP TABLE IF EXISTS consolidado_despesas CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
DROP TABLE IF EXISTS operadoras_cadastro CASCADE;
DROP TABLE IF EXISTS normalizacao CASCADE;

------------------------------------
--TABELA 1 -- CADASTRO DE OPERADORAS
------------------------------------
CREATE TABLE operadoras_cadastro (
    RegistroANS INTEGER PRIMARY KEY,  -- Chave primária
    CNPJ VARCHAR(20),
    RazaoSocial TEXT,
    Modalidade TEXT,
    UF CHAR(2)
);

-- Insere os novos registros em operadoras_cadastro
--registros que constavam em outras tabelas e não constavam na operadoras_cadastro, causando conflito de chave
INSERT INTO operadoras_cadastro (RegistroANS, CNPJ, RazaoSocial, Modalidade, UF)
VALUES
(477, '00000000000000', 'Operadora 477', 'Desconhecida', 'XX'),
(321338, '00000000000000', 'Operadora 321338', 'Desconhecida', 'XX'),
(345741, '00000000000000', 'Operadora 345741', 'Desconhecida', 'XX'),
(346870, '00000000000000', 'Operadora 346870', 'Desconhecida', 'XX'),
(349534, '00000000000000', 'Operadora 349534', 'Desconhecida', 'XX'),
(350141, '00000000000000', 'Operadora 350141', 'Desconhecida', 'XX'),
(351792, '00000000000000', 'Operadora 351792', 'Desconhecida', 'XX'),
(418161, '00000000000000', 'Operadora 418161', 'Desconhecida', 'XX'),
(419907, '00000000000000', 'Operadora 419907', 'Desconhecida', 'XX'),
(423475, '00000000000000', 'Operadora 423475', 'Desconhecida', 'XX'),
(423742, '00000000000000', 'Operadora 423742', 'Desconhecida', 'XX');

ALTER TABLE consolidado_despesas
ADD CONSTRAINT fk_consolidado_operadoras
FOREIGN KEY (RegistroANS)
REFERENCES operadoras_cadastro (RegistroANS);

------------------------------------
--TABELA 2 -- CONSOLIDADO DE DESPESAS
------------------------------------

-- tentativa de evitar redundância
CREATE TABLE consolidado_despesas (
    CNPJ VARCHAR(20),
    RegistroANS INTEGER,
    Trimestre VARCHAR(5),
    Ano INTEGER,
    ValorDespesas NUMERIC(18,2),
    PRIMARY KEY (RegistroANS, Trimestre, Ano)  -- Garante que não haverá duplicidade de RegistroANS no mesmo trimestre/ano
);

-- modelo de consulta vinculando dados:
SELECT cd.RegistroANS, cd.Trimestre, cd.Ano, cd.ValorDespesas, 
       oc.CNPJ, oc.RazaoSocial, oc.Modalidade, oc.UF
FROM consolidado_despesas cd
JOIN operadoras_cadastro oc ON cd.RegistroANS = oc.RegistroANS
ORDER BY cd.RegistroANS, cd.Ano, cd.Trimestre;


------------------------------------
--TABELA 3 -- DESPESAS AGREGADAS
------------------------------------

CREATE TABLE despesas_agregadas (
    RegistroANS INTEGER,  -- Chave estrangeira vinculada a operadoras_cadastro
    RazaoSocial TEXT,
    UF CHAR(2),
    TotalDespesas NUMERIC(18,2),
    MediaTrimestral NUMERIC(18,2),
    DesvioPadrao NUMERIC(18,2),
    PRIMARY KEY (RegistroANS),  -- Caso cada RegistroANS seja único
    FOREIGN KEY (RegistroANS) REFERENCES operadoras_cadastro(RegistroANS)
);

-- modelo de consulta vinculando dados:
SELECT da.RegistroANS, da.RazaoSocial, da.UF, da.TotalDespesas, da.MediaTrimestral, da.DesvioPadrao,
       oc.CNPJ, oc.Modalidade
FROM despesas_agregadas da
JOIN operadoras_cadastro oc ON da.RegistroANS = oc.RegistroANS
ORDER BY da.RegistroANS;



------------------------------------
-- TABELA 4 - CONSULTA JOIN DAS 3 TABELAS
------------------------------------
CREATE TABLE resultado_despesas AS
SELECT 
    oc.RegistroANS,
    oc.CNPJ,
    oc.RazaoSocial,
    oc.Modalidade,
    oc.UF,
    cd.Trimestre,
    cd.Ano,
    cd.ValorDespesas,
    da.TotalDespesas,
    da.MediaTrimestral,
    da.DesvioPadrao
FROM 
    operadoras_cadastro oc
JOIN 
    consolidado_despesas cd ON oc.RegistroANS = cd.RegistroANS
LEFT JOIN 
    despesas_agregadas da ON oc.RegistroANS = da.RegistroANS
ORDER BY 
    oc.RegistroANS, cd.Ano, cd.Trimestre;






