-- stg_rxnorm__brand_product_component_links.sql
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
    DISTINCT product.rxcui AS brand_product_rxcui,
    CASE
        WHEN product_component.rxcui IS NULL THEN product.rxcui
        ELSE product_component.rxcui
    END AS brand_product_component_rxcui
FROM
    product
    LEFT JOIN rxnrel
    ON rxnrel.rxcui2 = product.rxcui
    AND rxnrel.rela = 'contains'
    LEFT JOIN product_component
    ON rxnrel.rxcui1 = product_component.rxcui
    AND product_component.tty IN (
        'SBD',
        'SCD'
    ) -- NOTE: BPCKs can contain SBDs AND SCDs
    AND product_component.sab = 'RXNORM'
WHERE
    product.tty IN (
        'SBD',
        'BPCK'
    )
    AND product.sab = 'RXNORM'
