-- stg_rxnorm__product_rxcuis
SELECT
    *
FROM
    {{ source(
        'rxnorm',
        'rxnorm_rxnconso'
    ) }}
WHERE
    sab = 'RXNORM'
    AND tty IN (
        'SCD',
        'SBD',
        'GPCK',
        'BPCK'
    )
