-- stg_rxnorm__ingredient_components.sql
WITH cte AS (
    SELECT
        *
    FROM
        {{ ref('stg_rxnorm__common_ingredient_component') }}
)
SELECT
    DISTINCT CASE
        WHEN cte.ingredient_component_rxcui IS NULL THEN cte.rxcui
        ELSE cte.ingredient_component_rxcui
    END rxcui,
    CASE
        WHEN cte.ingredient_component_name IS NULL THEN cte.str
        ELSE cte.ingredient_component_name
    END NAME,
    CASE
        WHEN cte.ingredient_component_tty IS NULL THEN cte.tty
        ELSE cte.ingredient_component_tty
    END tty,
    CASE
        WHEN CASE
            WHEN cte.ingredient_component_rxcui IS NULL THEN cte.suppress
            ELSE cte.ingredient_component_suppress
        END = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN CASE
            WHEN cte.ingredient_component_rxcui IS NULL THEN cte.cvf
            ELSE cte.ingredient_component_cvf
        END = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    cte
