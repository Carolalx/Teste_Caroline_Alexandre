-- 1. Limpeza de ambiente
DROP TABLE IF EXISTS consolidado_despesas CASCADE;
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
DROP TABLE IF EXISTS operadoras_cadastro CASCADE;

-- 2. Criação da Tabela de Cadastro (Dimensão)
CREATE TABLE operadoras_cadastro (
    RegistroANS INTEGER PRIMARY KEY,
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

-- 3. Criação da Tabela Consolidada (Fatos)
CREATE TABLE consolidado_despesas (
    CNPJ VARCHAR(20),
    RegistroANS INTEGER,
    Trimestre VARCHAR(5),
    Ano INTEGER,
    ValorDespesas NUMERIC(18,2)
);

-- 4. Criação da Tabela de Agregação (Estatísticas)
CREATE TABLE despesas_agregadas (
    RegistroANS INTEGER,
    RazaoSocial TEXT,
    UF CHAR(2),
    TotalDespesas NUMERIC(18,2),
    MediaTrimestral NUMERIC(18,2),
    DesvioPadrao NUMERIC(18,2)
);