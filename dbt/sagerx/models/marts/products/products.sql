with

rxnorm_products as (

    select * from {{ ref('stg_rxnorm__products') }}

),

rxnorm_psn as (

    select
        rxcui,
        str        
    from {{ source('rxnorm', 'rxnorm_rxnconso') }}
    where sab = 'RXNORM'
        and tty = 'PSN'

),

rxnorm_clinical_products_to_ingredients as (

    select * from {{ ref('int_rxnorm_clinical_products_to_ingredients') }}

)

select
    prod.rxcui as product_rxcui
    , prod.name as product_name
    , prod.tty as product_tty
    , psn.str as prescribable_name
    , case
        when prod.tty in ('SBD', 'BPCK') then 'brand'
        when prod.tty in ('SCD', 'GPCK') then 'generic'
        end as brand_vs_generic
    , substring(prod.name from '\[(.*)\]') as brand_name
    , cping.clinical_product_rxcui
    , cping.clinical_product_name
    , cping.clinical_product_tty
    , cping.ingredient_name
    -- strength - couldn't easily get strength at this grain - can if needed
    , cping.dose_form_name
    , prod.active
    , prod.prescribable
from rxnorm_products prod
left join rxnorm_clinical_products_to_ingredients cping
    on cping.clinical_product_rxcui = prod.clinical_product_rxcui
left join rxnorm_psn psn
    on psn.rxcui = prod.rxcui
