-- stg_rxnorm__products.sql
SELECT
    product.rxcui AS rxcui,
    product.str AS NAME,
    product.tty AS tty,
    CASE
        WHEN brand_product.rxcui IS NOT NULL THEN brand_product.clinical_product_rxcui
        ELSE product.rxcui
    END AS clinical_product_rxcui,
    CASE
        WHEN product.suppress = 'N' THEN TRUE
        ELSE FALSE
    END AS active,
    CASE
        WHEN product.cvf = '4096' THEN TRUE
        ELSE FALSE
    END AS prescribable
FROM
    {{ source(
        'rxnorm',
        'rxnorm_rxnconso'
    ) }}
    product
    LEFT JOIN {{ ref('stg_rxnorm__brand_products') }}
    brand_product
    ON product.rxcui = brand_product.rxcui
WHERE
    product.tty IN(
        'SCD',
        'GPCK',
        'SBD',
        'BPCK'
    )
    AND product.sab = 'RXNORM'
    /*
    with
    
    rcp as (
    
        select * from {{ ref('stg_rxnorm__clinical_products') }}
    
    ),
    
    rbp as (
    
        select * from {{ ref('stg_rxnorm__brand_products') }}
    
    )
    
    select distinct
        coalesce(rbp.rxcui, rcp.rxcui, null) as product_rxcui
        , coalesce(rbp.name, rcp.name, null) as product_name
        , coalesce(rbp.tty, rcp.tty, null) as product_tty
        , rcp.rxcui as clinical_product_rxcui
        , rcp.name as clinical_product_name
        , rcp.tty as clinical_product_tty
    from rcp
    left join rbp
        on rbp.clinical_product_rxcui = rcp.rxcui
    */
