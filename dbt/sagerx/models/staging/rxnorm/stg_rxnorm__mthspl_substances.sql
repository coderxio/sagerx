-- stg_rxnorm__mthspl_substances.sql
SELECT
    substance.rxcui rxcui,
    substance.str NAME,
    substance.tty tty,
    substance.rxaui rxaui,
    substance.code unii,
    CASE
        WHEN substance.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN substance.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    {{ source(
        'rxnorm',
        'rxnorm_rxnconso'
    ) }} AS substance
WHERE
    substance.tty = 'SU'
    AND substance.sab = 'MTHSPL'
