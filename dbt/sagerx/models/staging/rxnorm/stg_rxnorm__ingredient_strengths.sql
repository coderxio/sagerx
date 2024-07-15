-- stg_rxnorm__ingredient_strengths.sql
WITH ingredient_strength AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
),
rxnsat AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnsat'
        ) }}
)
SELECT
    ingredient_strength.rxcui AS rxcui,
    ingredient_strength.str AS NAME,
    numerator_value.atv AS numerator_value,
    numerator_unit.atv AS numerator_unit,
    denominator_value.atv AS denominator_value,
    denominator_unit.atv AS denominator_unit,
    text.atv AS text,
    CASE
        WHEN ingredient_strength.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN ingredient_strength.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    ingredient_strength
    LEFT JOIN rxnsat AS numerator_value
    ON numerator_value.rxcui = ingredient_strength.rxcui
    AND numerator_value.atn = 'RXN_BOSS_STRENGTH_NUM_VALUE'
    LEFT JOIN rxnsat AS numerator_unit
    ON numerator_unit.rxcui = ingredient_strength.rxcui
    AND numerator_unit.atn = 'RXN_BOSS_STRENGTH_NUM_UNIT'
    LEFT JOIN rxnsat AS denominator_value
    ON denominator_value.rxcui = ingredient_strength.rxcui
    AND denominator_value.atn = 'RXN_BOSS_STRENGTH_DENOM_VALUE'
    LEFT JOIN rxnsat AS denominator_unit
    ON denominator_unit.rxcui = ingredient_strength.rxcui
    AND denominator_unit.atn = 'RXN_BOSS_STRENGTH_DENOM_UNIT'
    LEFT JOIN rxnsat AS text
    ON text.rxcui = ingredient_strength.rxcui
    AND text.atn = 'RXN_STRENGTH'
WHERE
    ingredient_strength.tty = 'SCDC'
    AND ingredient_strength.sab = 'RXNORM'
