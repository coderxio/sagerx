-- stg_rxnorm__clinical_product_component_links.sql

with 

clinical_products as (
    select
        *
    from {{ ref('stg_rxnorm__clinical_products') }}
),

rxnrel as (

    select
        rxcui2 as clinical_product_rxcui,
        rxcui1 as clinical_product_component_rxcui

    from
        {{ ref(
            'stg_rxnorm__rxnrel_ttys'
        ) }}
    
    where tty2 = 'GPCK'
        and rela = 'contains'
        and tty1 = 'SCD'

),

clinical_product_component_links as (

    select
        clinical_products.rxcui as clinical_product_rxcui,
        case
            when clinical_product_component_rxcui is not null
                then clinical_product_component_rxcui
            else clinical_products.rxcui
            end as clinical_product_component_rxcui
    from clinical_products
    left join rxnrel
        on clinical_product_rxcui 
            = clinical_products.rxcui

)

select * from clinical_product_component_links
