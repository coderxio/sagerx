-- stg_rxnorm__brand_product_components.sql
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
    DISTINCT CASE
        WHEN product.tty = 'SBD' THEN product.rxcui
        ELSE product_component.rxcui
    END rxcui,
    CASE
        WHEN product.tty = 'SBD' THEN product.str
        ELSE product_component.str
    END NAME,
    CASE
        WHEN product.tty = 'SBD' THEN product.tty
        ELSE product_component.tty
    END tty,
    CASE
        WHEN product_component.tty = 'SCD' THEN product_component.rxcui
        ELSE rxnrel_scd.rxcui1
    END clinical_product_component_rxcui,
    rxnrel_bn.rxcui1 AS brand_rxcui,
    CASE
        WHEN CASE
            WHEN product.tty = 'SBD' THEN product.suppress
            ELSE product_component.suppress
        END = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN CASE
            WHEN product.tty = 'SBD' THEN product.cvf
            ELSE product_component.cvf
        END = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
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
    LEFT JOIN rxnrel AS rxnrel_scd
    ON rxnrel_scd.rxcui2 = CASE
        WHEN product_component.rxcui IS NULL THEN product.rxcui
        ELSE product_component.rxcui
    END
    AND rxnrel_scd.rela = 'tradename_of' -- rxnrel_scd.rxcui1 = clinical_product_component_rxcui
    LEFT JOIN rxnrel AS rxnrel_bn
    ON rxnrel_bn.rxcui2 = CASE
        WHEN product_component.rxcui IS NULL THEN product.rxcui
        ELSE product_component.rxcui
    END
    AND rxnrel_bn.rela = 'has_ingredient' -- rxnrel_bn.rxcui1 = brand_rxcui
WHERE
    product.tty IN (
        'SBD',
        'BPCK'
    )
    AND product.sab = 'RXNORM'
