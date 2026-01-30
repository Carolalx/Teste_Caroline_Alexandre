-- #region 1. Quais as 5 operadoras com maior crescimento percentual (...)
WITH trimestres AS (
    -- Obtem o primeiro e o último trimestre disponíveis para cada operadora
    SELECT 
        RegistroANS,
        MIN(Ano || '-' || Trimestre) AS primeiro_trimestre,
        MAX(Ano || '-' || Trimestre) AS ultimo_trimestre
    FROM consolidado_despesas
    GROUP BY RegistroANS
),
valores_trimestres AS (
    -- Calcula o total de despesas por trimestre (somando caso haja mais de uma linha)
    SELECT 
        cd.RegistroANS,
        cd.Trimestre,
        cd.Ano,
        SUM(cd.ValorDespesas) AS total_despesas
    FROM consolidado_despesas cd
    GROUP BY cd.RegistroANS, cd.Ano, cd.Trimestre
),
crescimento AS (
    -- Pega despesas do primeiro e último trimestre e calculamos crescimento percentual
    SELECT
        t.RegistroANS,
        oc.RazaoSocial,
        vt_ini.total_despesas AS despesas_iniciais,
        vt_fim.total_despesas AS despesas_finais,
        ((vt_fim.total_despesas - vt_ini.total_despesas) / vt_ini.total_despesas) * 100 AS crescimento_percentual
    FROM trimestres t
    JOIN valores_trimestres vt_ini
        ON t.RegistroANS = vt_ini.RegistroANS
       AND (vt_ini.Ano || '-' || vt_ini.Trimestre) = t.primeiro_trimestre
    JOIN valores_trimestres vt_fim
        ON t.RegistroANS = vt_fim.RegistroANS
       AND (vt_fim.Ano || '-' || vt_fim.Trimestre) = t.ultimo_trimestre
    JOIN operadoras_cadastro oc
        ON t.RegistroANS = oc.RegistroANS
)
-- Selecionamos as 5 operadoras com maior crescimento percentual
SELECT *
FROM crescimento
ORDER BY crescimento_percentual DESC
LIMIT 5;
-- #endregion 

-- #region Distribuição de Despesas po UF
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

-- #region Operadoras acima da média (ranking)
WITH cte_media AS (
    SELECT Ano, Trimestre, AVG(ValorDespesas) AS media_trimestre
    FROM consolidado_despesas
    GROUP BY Ano, Trimestre
),
cte_operadora_trimestre AS (
    SELECT
        cd.RegistroANS,
        cd.Ano,
        cd.Trimestre,
        SUM(cd.ValorDespesas) AS total_despesas
    FROM consolidado_despesas cd
    GROUP BY cd.RegistroANS, cd.Ano, cd.Trimestre
),
cte_acima_media AS (
    SELECT
        ot.RegistroANS,
        oc.RazaoSocial,
        ot.total_despesas,
        CASE WHEN ot.total_despesas > cm.media_trimestre THEN 1 ELSE 0 END AS acima_media
    FROM cte_operadora_trimestre ot
    JOIN cte_media cm
        ON ot.Ano = cm.Ano AND ot.Trimestre = cm.Trimestre
    JOIN operadoras_cadastro oc
        ON ot.RegistroANS = oc.RegistroANS
),
cte_count_trimestres AS (
    SELECT
        RegistroANS,
        RazaoSocial,
        SUM(acima_media) AS trimestres_acima_media,
        SUM(total_despesas) AS total_despesas_acima_media
    FROM cte_acima_media
    GROUP BY RegistroANS, RazaoSocial
)
SELECT 
    RegistroANS,
    RazaoSocial,
    trimestres_acima_media,
    total_despesas_acima_media,
    RANK() OVER (ORDER BY trimestres_acima_media DESC, total_despesas_acima_media DESC) AS ranking
FROM cte_count_trimestres
WHERE trimestres_acima_media >= 1
ORDER BY ranking;
-- #endregion