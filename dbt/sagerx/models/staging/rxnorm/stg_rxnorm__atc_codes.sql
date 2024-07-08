WITH atc AS (
    SELECT
        DISTINCT A.rxcui,
        A.code,
        b.atn,
        b.atv AS atc_class_level,
        A.str AS description,
        A.sab,
        A.tty
    FROM
        (
            SELECT
                *
            FROM
                {{ source(
                    'rxnorm',
                    'rxnorm_rxnconso'
                ) }}
            WHERE
                sab = 'ATC'
                AND tty NOT LIKE 'RXN%'
            ORDER BY
                code
        ) A
        LEFT JOIN {{ source(
            'rxnorm',
            'rxnorm_rxnsat'
        ) }} AS b
        ON A.code = b.code
    WHERE
        atn = 'ATC_LEVEL'
    ORDER BY
        code
),
atc_5 AS (
    SELECT
        *
    FROM
        atc
    WHERE
        atc_class_level = '5'
),
atc_4 AS (
    SELECT
        *
    FROM
        atc
    WHERE
        atc_class_level = '4'
),
atc_3 AS (
    SELECT
        *
    FROM
        atc
    WHERE
        atc_class_level = '3'
),
atc_2 AS (
    SELECT
        *
    FROM
        atc
    WHERE
        atc_class_level = '2'
),
atc_1 AS (
    SELECT
        *
    FROM
        atc
    WHERE
        atc_class_level = '1'
),
sagerx_atc AS (
    SELECT
        atc_1.code AS atc_1_code,
        atc_1.description AS atc_1_name,
        atc_2.code AS atc_2_code,
        atc_2.description AS atc_2_name,
        atc_3.code AS atc_3_code,
        atc_3.description AS atc_3_name,
        atc_4.code AS atc_4_code,
        atc_4.description AS atc_4_name,
        atc_5.code AS atc_5_code,
        atc_5.description AS atc_5_name,
        atc_5.rxcui AS ingredient_rxcui,
        atc_5.description AS ingredient_name,
        atc_5.tty AS ingredient_tty
    FROM
        atc_5
        LEFT JOIN atc_4
        ON LEFT(
            atc_5.code,
            5
        ) = atc_4.code
        LEFT JOIN atc_3
        ON LEFT(
            atc_4.code,
            4
        ) = atc_3.code
        LEFT JOIN atc_2
        ON LEFT(
            atc_3.code,
            3
        ) = atc_2.code
        LEFT JOIN atc_1
        ON LEFT(
            atc_2.code,
            1
        ) = atc_1.code
)
SELECT
    *
FROM
    sagerx_atc
