-- stg_rxnorm__dose_forms.sql
WITH dose_form AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    dose_form.rxcui rxcui,
    dose_form.str NAME,
    dose_form.tty tty,
    CASE
        WHEN dose_form.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN dose_form.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    dose_form
WHERE
    dose_form.tty = 'DF'
    AND dose_form.sab = 'RXNORM'
