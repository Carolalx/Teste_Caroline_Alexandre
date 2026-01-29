-- ATENÇÃO: Execute este script APÓS importar os CSVs via assistente do DBeaver

-- 1. Correção da escala decimal (ajuste de centavos ignorados na importação)
UPDATE consolidado_despesas SET ValorDespesas = ValorDespesas / 100;

UPDATE despesas_agregadas SET 
    TotalDespesas = TotalDespesas / 100,
    MediaTrimestral = MediaTrimestral / 100,
    DesvioPadrao = DesvioPadrao / 100;

-- 2. Criando índices para performance em consultas grandes
CREATE INDEX idx_registro_ans_cons ON consolidado_despesas(RegistroANS);
CREATE INDEX idx_registro_ans_cad ON operadoras_cadastro(RegistroANS);

-- 3. Verificação de integridade (Identificar operadoras sem cadastro)
SELECT DISTINCT RegistroANS 
FROM consolidado_despesas 
WHERE RegistroANS NOT IN (SELECT RegistroANS FROM operadoras_cadastro);