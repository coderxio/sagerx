-- stg_rxnorm__dose_form_groups.sql
WITH dose_form_group AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    dose_form_group.rxcui rxcui,
    dose_form_group.str NAME,
    dose_form_group.tty tty,
    CASE
        WHEN dose_form_group.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN dose_form_group.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    dose_form_group
WHERE
    dose_form_group.tty = 'DFG'
    AND dose_form_group.sab = 'RXNORM'
