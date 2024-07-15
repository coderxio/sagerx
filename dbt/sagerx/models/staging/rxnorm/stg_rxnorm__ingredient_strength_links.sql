-- stg_rxnorm__ingredient_strength_links.sql
WITH rxnrel AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }}
),
ingredient AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    DISTINCT product_component.rxcui AS clinical_product_component_rxcui,
    ingredient.rxcui AS ingredient_component_rxcui,
    ingredient_strength.rxcui AS ingredient_strength_rxcui
FROM
    ingredient AS ingredient_strength
    INNER JOIN rxnrel AS has_ingredient
    ON has_ingredient.rxcui2 = ingredient_strength.rxcui
    AND has_ingredient.rela = 'has_ingredient'
    INNER JOIN ingredient
    ON ingredient.rxcui = has_ingredient.rxcui1
    AND ingredient.tty = 'IN'
    AND ingredient.sab = 'RXNORM'
    INNER JOIN rxnrel AS constitutes
    ON constitutes.rxcui2 = ingredient_strength.rxcui
    AND constitutes.rela = 'constitutes'
    INNER JOIN ingredient AS product_component
    ON product_component.rxcui = constitutes.rxcui1
    AND product_component.tty = 'SCD'
    AND product_component.sab = 'RXNORM'
WHERE
    ingredient_strength.tty = 'SCDC'
    AND ingredient_strength.sab = 'RXNORM'
