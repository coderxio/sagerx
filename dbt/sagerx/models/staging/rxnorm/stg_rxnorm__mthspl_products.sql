-- stg_rxnorm__mthspl_products.sql
SELECT
    product.rxcui AS rxcui,
    product.str AS NAME,
    product.tty AS tty,
    product.rxaui AS rxaui,
    product.code AS ndc,
    CASE
        WHEN product.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN product.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    {{ source(
        'rxnorm',
        'rxnorm_rxnconso'
    ) }} AS product
WHERE
    product.tty = 'DP'
    AND product.sab = 'MTHSPL'
