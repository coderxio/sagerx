-- stg_rxnorm__precise_ingredients.sql
WITH ingredient AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    ingredient.rxcui rxcui,
    ingredient.str NAME,
    ingredient.tty tty,
    CASE
        WHEN ingredient.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN ingredient.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    ingredient
WHERE
    ingredient.tty = 'PIN'
    AND ingredient.sab = 'RXNORM'
