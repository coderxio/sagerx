-- stg_rxnorm__clinical_product_components.sql
WITH product AS (
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
product_component AS (
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
            PARTITION BY product_component_rxcui
            ORDER BY
                ingredient_tty DESC
        ) AS rn
    FROM
        (
            SELECT
                product_component.rxcui AS product_component_rxcui,
                product_component.str AS product_component_name,
                product_component.tty AS product_component_tty,
                ingredient.rxcui AS ingredient_rxcui,
                ingredient.str AS ingredient_name,
                ingredient.tty AS ingredient_tty
            FROM
                product_component
                INNER JOIN rxnrel
                ON rxnrel.rxcui2 = product_component.rxcui
                AND rxnrel.rela = 'has_ingredients'
                INNER JOIN ingredient
                ON rxnrel.rxcui1 = ingredient.rxcui
                AND ingredient.tty = 'MIN'
                AND ingredient.sab = 'RXNORM'
            WHERE
                product_component.tty = 'SCD'
                AND product_component.sab = 'RXNORM'
            UNION ALL
            SELECT
                product_component.rxcui AS product_component_rxcui,
                product_component.str AS product_component_name,
                product_component.tty AS product_component_tty,
                ingredient.rxcui AS ingredient_rxcui,
                ingredient.str AS ingredient_name,
                ingredient.tty AS ingredient_tty
            FROM
                product_component
                INNER JOIN rxnrel AS scdc_rxnrel
                ON scdc_rxnrel.rxcui2 = product_component.rxcui
                AND scdc_rxnrel.rela = 'consists_of'
                INNER JOIN product AS scdc
                ON scdc_rxnrel.rxcui1 = scdc.rxcui
                INNER JOIN rxnrel AS ingredient_rxnrel
                ON ingredient_rxnrel.rxcui2 = scdc.rxcui
                AND ingredient_rxnrel.rela = 'has_ingredient'
                INNER JOIN ingredient
                ON ingredient_rxnrel.rxcui1 = ingredient.rxcui
                AND ingredient.tty = 'IN'
                AND ingredient.sab = 'RXNORM'
            WHERE
                product_component.tty = 'SCD'
                AND product_component.sab = 'RXNORM'
        ) sq
)
SELECT
    DISTINCT CASE
        WHEN product_component.rxcui IS NULL THEN product.rxcui
        ELSE product_component.rxcui
    END rxcui,
    CASE
        WHEN product_component.str IS NULL THEN product.str
        ELSE product_component.str
    END NAME,
    CASE
        WHEN product_component.tty IS NULL THEN product.tty
        ELSE product_component.tty
    END tty,
    CASE
        WHEN CASE
            WHEN product_component.rxcui IS NULL THEN product.suppress
            ELSE product_component.suppress
        END = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN CASE
            WHEN product_component.rxcui IS NULL THEN product.cvf
            ELSE product_component.cvf
        END = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable,
    cte.ingredient_rxcui AS ingredient_rxcui,
    dose_form_rxnrel.rxcui1 AS dose_form_rxcui
FROM
    product
    LEFT JOIN rxnrel
    ON rxnrel.rxcui2 = product.rxcui
    AND rxnrel.rela = 'contains'
    LEFT JOIN product_component
    ON rxnrel.rxcui1 = product_component.rxcui
    AND product_component.tty = 'SCD'
    AND product_component.sab = 'RXNORM'
    LEFT JOIN cte
    ON cte.product_component_rxcui = CASE
        WHEN product_component.rxcui IS NULL THEN product.rxcui
        ELSE product_component.rxcui
    END
    AND cte.rn < 2
    LEFT JOIN rxnrel AS dose_form_rxnrel
    ON dose_form_rxnrel.rxcui2 = CASE
        WHEN product_component.rxcui IS NULL THEN product.rxcui
        ELSE product_component.rxcui
    END
    AND dose_form_rxnrel.rela = 'has_dose_form'
    AND dose_form_rxnrel.sab = 'RXNORM'
WHERE
    product.tty IN(
        'SCD',
        'GPCK'
    )
    AND product.sab = 'RXNORM'
