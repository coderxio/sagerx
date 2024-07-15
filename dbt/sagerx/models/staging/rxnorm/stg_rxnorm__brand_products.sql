-- stg_rxnorm__brand_products.sql
WITH product AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
),
rxnrel AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }}
),
product_component AS (
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
    product_component.rxcui AS clinical_product_rxcui,
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
    LEFT JOIN rxnrel
    ON rxnrel.rxcui2 = product.rxcui
    AND rxnrel.rela = 'tradename_of'
    LEFT JOIN product_component
    ON rxnrel.rxcui1 = product_component.rxcui
    AND product_component.tty IN (
        'SCD',
        'GPCK'
    )
    AND product_component.sab = 'RXNORM'
WHERE
    product.tty IN(
        'SBD',
        'BPCK'
    )
    AND product.sab = 'RXNORM'
