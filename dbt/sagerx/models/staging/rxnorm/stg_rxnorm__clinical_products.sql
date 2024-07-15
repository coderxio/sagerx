-- stg_rxnorm__clinical_products.sql
WITH product AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    product.rxcui AS rxcui,
    product.str AS NAME,
    product.tty AS tty,
    CASE
        WHEN product.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN product.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    product
WHERE
    product.tty IN(
        'SCD',
        'GPCK'
    )
    AND product.sab = 'RXNORM'
