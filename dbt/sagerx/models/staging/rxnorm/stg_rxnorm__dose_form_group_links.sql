-- stg_rxnorm__dose_form_group_links.sql
WITH rxnrel AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }}
),
dose_form AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
)
SELECT
    DISTINCT dose_form.rxcui dose_form_rxcui,
    rxnrel.rxcui1 dose_form_group_rxcui
FROM
    dose_form
    INNER JOIN rxnrel
    ON rxnrel.rxcui2 = dose_form.rxcui
    AND rxnrel.rela = 'isa'
    AND rxnrel.sab = 'RXNORM'
WHERE
    dose_form.tty = 'DF'
    AND dose_form.sab = 'RXNORM'
