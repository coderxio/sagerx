-- stg_rxnorm__ingredients.sql
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
    {{ source(
        'rxnorm',
        'rxnorm_rxnconso'
    ) }} AS ingredient
WHERE
    ingredient.tty IN(
        'IN',
        'MIN'
    )
    AND ingredient.sab = 'RXNORM'
