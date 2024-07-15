-- stg_rxnorm__brands.sql
WITH brand AS (
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
ingredient AS (
    SELECT
        *
    FROM
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}
),
cte AS (
    SELECT
        sq.*,
        ROW_NUMBER() over(
            PARTITION BY rxcui
            ORDER BY
                ingredient_tty DESC
        ) AS rn
    FROM
        (
            SELECT
                brand.rxcui AS rxcui,
                brand.str AS NAME,
                brand.tty AS tty,
                ingredient.rxcui AS ingredient_rxcui,
                ingredient.str AS ingredient_name,
                ingredient.tty AS ingredient_tty
            FROM
                brand
                INNER JOIN rxnrel
                ON rxnrel.rxcui2 = brand.rxcui
                AND rxnrel.rela = 'tradename_of'
                INNER JOIN ingredient
                ON rxnrel.rxcui1 = ingredient.rxcui
                AND ingredient.tty = 'IN'
                AND ingredient.sab = 'RXNORM'
            WHERE
                brand.tty = 'BN'
                AND brand.sab = 'RXNORM'
            UNION ALL
            SELECT
                brand.rxcui AS rxcui,
                brand.str AS NAME,
                brand.tty AS tty,
                ingredient.rxcui AS ingredient_rxcui,
                ingredient.str AS ingredient_name,
                ingredient.tty AS ingredient_tty
            FROM
                brand
                INNER JOIN rxnrel AS sbd_rxnrel
                ON sbd_rxnrel.rxcui2 = brand.rxcui
                AND sbd_rxnrel.rela = 'ingredient_of'
                INNER JOIN rxnrel AS scd_rxnrel
                ON scd_rxnrel.rxcui2 = sbd_rxnrel.rxcui1
                AND scd_rxnrel.rela = 'tradename_of'
                INNER JOIN rxnrel AS ingredient_rxnrel
                ON ingredient_rxnrel.rxcui2 = scd_rxnrel.rxcui1
                AND ingredient_rxnrel.rela = 'has_ingredients'
                LEFT JOIN ingredient
                ON ingredient_rxnrel.rxcui1 = ingredient.rxcui
                AND ingredient.tty = 'MIN'
                AND ingredient.sab = 'RXNORM'
            WHERE
                brand.tty = 'BN'
                AND brand.sab = 'RXNORM'
        ) sq
)
SELECT
    DISTINCT brand.rxcui AS rxcui,
    brand.str AS NAME,
    brand.tty AS tty,
    CASE
        WHEN brand.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN brand.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable,
    cte.ingredient_rxcui AS ingredient_rxcui
FROM
    brand AS product
    INNER JOIN rxnrel
    ON rxnrel.rxcui2 = product.rxcui
    AND rxnrel.rela = 'has_ingredient'
    INNER JOIN brand
    ON rxnrel.rxcui1 = brand.rxcui
    AND brand.tty = 'BN'
    AND brand.sab = 'RXNORM'
    LEFT JOIN cte
    ON cte.rxcui = brand.rxcui
    AND cte.rn < 2
WHERE
    product.tty = 'SBD'
    AND product.sab = 'RXNORM'
