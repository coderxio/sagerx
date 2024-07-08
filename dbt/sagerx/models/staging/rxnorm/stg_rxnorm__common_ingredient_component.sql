-- stg_rxnorm__common_ingredient_component.sql

WITH rxnrel AS (
    SELECT * FROM {{ source('rxnorm', 'rxnorm_rxnrel') }}
)

, ingredient_component AS (
    SELECT * FROM {{ source('rxnorm', 'rxnorm_rxnconso') }}
)

, cte AS (
    SELECT
        rxnrel.rxcui2 AS ingredient_rxcui
        , ingredient_component.rxcui AS rxcui
        , ingredient_component.str AS name
        , ingredient_component.tty AS tty
        , ingredient_component.suppress
        , ingredient_component.cvf
    FROM rxnrel
    INNER JOIN ingredient_component
        ON rxnrel.rxcui1 = ingredient_component.rxcui
    WHERE rxnrel.rela = 'hAS_part'
        AND ingredient_component.tty = 'IN'
        AND ingredient_component.sab = 'RXNORM'
)

SELECT
    ingredient_component.*
    , cte.ingredient_rxcui
    , cte.rxcui AS ingredient_component_rxcui
    , cte.name AS ingredient_component_name
    , cte.tty AS ingredient_component_tty
    , cte.suppress AS ingredient_component_suppress
    , cte.cvf AS ingredient_component_cvf
FROM ingredient_component
LEFT JOIN cte 
    ON ingredient_component.rxcui = cte.ingredient_rxcui
WHERE ingredient_component.tty IN ('IN', 'MIN')
    AND ingredient_component.sab = 'RXNORM'
