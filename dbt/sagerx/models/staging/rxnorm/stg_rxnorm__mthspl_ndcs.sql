-- stg_rxnorm__mthspl_ndcs.sql
SELECT
    {{ ndc_to_11 ('rxnsat.atv') }} AS ndc11,
    rxnsat.atv AS ndc,
    rxnsat.rxcui,
    CASE
        WHEN rxnsat.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN rxnsat.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    {{ source(
        'rxnorm',
        'rxnorm_rxnsat'
    ) }} AS rxnsat
WHERE
    rxnsat.atn = 'NDC'
    AND rxnsat.sab = 'MTHSPL'
