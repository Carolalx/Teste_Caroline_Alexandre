-- #region QUERY 1 - Quais as 5 operadoras com maior crescimento percentual (...)
WITH primeiro_ultimo AS (
    SELECT 
        RegistroANS,
        RazaoSocial,
        MIN(Ano || '-' || Trimestre) AS primeiro_trimestre,
        MAX(Ano || '-' || Trimestre) AS ultimo_trimestre
    FROM resultado_despesas
    GROUP BY RegistroANS, RazaoSocial
),
valores_trimestre AS (
    SELECT
        RegistroANS,
        RazaoSocial,
        ValorDespesas,
        Ano || '-' || Trimestre AS ano_trimestre,
        Ano,
        Trimestre
    FROM resultado_despesas
)
SELECT 
    p.RegistroANS,
    p.RazaoSocial,
    v_ini.ValorDespesas AS ValorInicial,
    v_fim.ValorDespesas AS ValorFinal,
    ROUND(
        ((v_fim.ValorDespesas - v_ini.ValorDespesas) / v_ini.ValorDespesas) * 100
    , 2) AS Crescimento_Percentual
FROM primeiro_ultimo p
JOIN valores_trimestre v_ini
    ON v_ini.RegistroANS = p.RegistroANS 
    AND v_ini.ano_trimestre = p.primeiro_trimestre
JOIN valores_trimestre v_fim
    ON v_fim.RegistroANS = p.RegistroANS 
    AND v_fim.ano_trimestre = p.ultimo_trimestre
WHERE v_ini.ValorDespesas <> 0  -- evita divisão por zero
ORDER BY Crescimento_Percentual DESC
LIMIT 5;


-- #endregion 

-- #region QUERY 2 - Distribuição de Despesas po UF
WITH despesas_por_operadora AS (
    -- Total de despesas de cada operadora por UF
    SELECT 
        oc.UF,
        cd.RegistroANS,
        SUM(cd.ValorDespesas) AS total_despesas_operadora
    FROM consolidado_despesas cd
    JOIN operadoras_cadastro oc
        ON cd.RegistroANS = oc.RegistroANS
    GROUP BY oc.UF, cd.RegistroANS
),
despesas_por_uf AS (
    -- Total e média de despesas por UF
    SELECT
        UF,
        SUM(total_despesas_operadora) AS total_despesas_uf,
        AVG(total_despesas_operadora) AS media_despesas_por_operadora
    FROM despesas_por_operadora
    GROUP BY UF
)
-- Seleciona os 5 estados com maiores despesas totais
SELECT *
FROM despesas_por_uf
ORDER BY total_despesas_uf DESC
LIMIT 5;
-- #endregion

-- #region  QUERY 3 - Operadoras acima da média (ranking)
WITH media_geral AS (
    SELECT AVG(ValorDespesas) AS media
    FROM resultado_despesas
),
trimestres_acima AS (
    SELECT 
        RegistroANS,
        RazaoSocial,
        Ano,
        Trimestre,
        CASE WHEN ValorDespesas > (SELECT media FROM media_geral) THEN 1 ELSE 0 END AS acima_media,
        ValorDespesas
    FROM resultado_despesas
),
contagem_trimestres AS (
    SELECT 
        RegistroANS,
        RazaoSocial,
        SUM(acima_media) AS trimestres_acima_media,
        SUM(ValorDespesas) AS total_despesas_acima_media
    FROM trimestres_acima
    GROUP BY RegistroANS, RazaoSocial
)
SELECT 
    RegistroANS,
    RazaoSocial,
    trimestres_acima_media,
    total_despesas_acima_media,
    RANK() OVER (
        ORDER BY trimestres_acima_media DESC, total_despesas_acima_media DESC
    ) AS ranking
FROM contagem_trimestres
WHERE trimestres_acima_media >= 2
ORDER BY ranking;
-- #endregion