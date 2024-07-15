-- stg_rxnorm__ndcs.sql
WITH rxnsat AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnsat'
        ) }}
),
product AS (
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
clinical_product AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    rxnsat.atv AS ndc,CASE
        WHEN product.tty IN (
            'BPCK',
            'SBD'
        ) THEN clinical_product.rxcui
        ELSE rxnsat.rxcui
    END AS clinical_product_rxcui,CASE
        WHEN product.tty IN (
            'BPCK',
            'SBD'
        ) THEN rxnsat.rxcui
        ELSE NULL
    END AS brand_product_rxcui,
    CASE
        WHEN rxnsat.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN rxnsat.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    rxnsat
    INNER JOIN product
    ON rxnsat.rxaui = product.rxaui
    LEFT JOIN rxnrel
    ON rxnsat.rxcui = rxnrel.rxcui2
    AND rela = 'tradename_of'
    AND product.tty IN (
        'BPCK',
        'SBD'
    )
    LEFT JOIN clinical_product
    ON rxnrel.rxcui1 = clinical_product.rxcui
    AND clinical_product.tty IN (
        'SCD',
        'GPCK'
    )
    AND clinical_product.sab = 'RXNORM'
WHERE
    rxnsat.atn = 'NDC'
    AND rxnsat.sab IN (
        'ATC',
        'CVX',
        'DRUGBANK',
        'MSH',
        'MTHCMSFRF',
        'MTHSPL',
        'RXNORM',
        'USP',
        'VANDF'
    )
    AND product.tty IN (
        'SCD',
        'SBD',
        'GPCK',
        'BPCK'
    )
    AND product.sab = 'RXNORM'
