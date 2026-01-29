-- 1. Top 10 operadoras com maiores despesas totais
SELECT 
    cad.RazaoSocial, 
    cad.Modalidade, 
    agg.TotalDespesas 
FROM despesas_agregadas agg
LEFT JOIN operadoras_cadastro cad ON agg.RegistroANS = cad.RegistroANS
ORDER BY agg.TotalDespesas DESC
LIMIT 10;

-- 2. Distribuição de despesas por UF
SELECT 
    UF, 
    SUM(TotalDespesas) as Gasto_Por_Estado
FROM despesas_agregadas
GROUP BY UF
ORDER BY Gasto_Por_Estado DESC;

-- 3. Média de gastos por modalidade de operadora
SELECT 
    cad.Modalidade, 
    ROUND(AVG(agg.MediaTrimestral), 2) as Media_Salarial_Modalidade
FROM despesas_agregadas agg
JOIN operadoras_cadastro cad ON agg.RegistroANS = cad.RegistroANS
GROUP BY cad.Modalidade
ORDER BY Media_Salarial_Modalidade DESC;