-- stg_rxnorm__precise_ingredient_links.sql
WITH precise_ingredient AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
),
precise_ingredient_of AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }}
),
ingredient_strength AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    DISTINCT ingredient_strength.rxcui AS ingredient_strength_rxcui,
    precise_ingredient.rxcui AS precise_ingredient_rxcui
FROM
    precise_ingredient
    INNER JOIN precise_ingredient_of
    ON precise_ingredient_of.rxcui2 = precise_ingredient.rxcui
    AND precise_ingredient_of.rela = 'precise_ingredient_of'
    INNER JOIN ingredient_strength
    ON ingredient_strength.rxcui = precise_ingredient_of.rxcui1
    AND ingredient_strength.tty = 'SCDC'
    AND ingredient_strength.sab = 'RXNORM'
WHERE
    precise_ingredient.tty = 'PIN'
    AND precise_ingredient.sab = 'RXNORM'
