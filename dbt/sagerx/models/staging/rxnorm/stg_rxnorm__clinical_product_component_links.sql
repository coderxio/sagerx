-- stg_rxnorm__clinical_product_component_links.sql
with clinical_products as (

    select
        rxcui

    from
        {{ ref(
            'stg_rxnorm__clinical_products'
        ) }}

),

clinical_product_components as (

    select
        rxcui

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnconso'
        ) }}

        where tty = 'SCD' and
            sab = 'RXNORM'

),

clinical_product_component_links as (

    select distinct 
        clinical_products.rxcui as clinical_product_rxcui,
        case
            when clinical_product_components.rxcui is null
                then clinical_products.rxcui
            else clinical_product_components.rxcui
        end as clinical_product_component_rxcui

    from
        {{ source(
            'rxnorm',
            'rxnorm_rxnrel'
        ) }} rxnrel
    
    left join clinical_products
        on clinical_products.rxcui = rxnrel.rxcui2
    left join clinical_product_components
        on clinical_product_components.rxcui = rxnrel.rxcui1

    where rxnrel.rela = 'contains'

    group by
        1,
        2

)

select * from clinical_product_component_links
