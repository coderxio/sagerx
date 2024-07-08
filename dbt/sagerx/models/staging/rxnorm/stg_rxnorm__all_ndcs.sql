-- stg_rxnorm__all_ndcs.sql
WITH rxnsat AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnsat'
        ) }}
)
SELECT
    {{ ndc_to_11 ('rxnsat.atv') }} AS ndc11,
    rxnsat.atv AS ndc,
    rxnsat.rxcui,
    rxnsat.sab,
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
