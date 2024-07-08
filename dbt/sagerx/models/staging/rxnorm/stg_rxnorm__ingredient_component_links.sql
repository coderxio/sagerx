-- stg_rxnorm__ingredient_component_links.sql
WITH cte AS (
    SELECT
        *
    FROM
        {{ ref('stg_rxnorm__common_ingredient_component') }}
)
SELECT
    DISTINCT cte.rxcui AS ingredient_rxcui,
    CASE
        WHEN cte.ingredient_component_rxcui IS NULL THEN cte.rxcui
        ELSE cte.ingredient_component_rxcui
    END AS ingredient_component_rxcui
FROM
    cte
