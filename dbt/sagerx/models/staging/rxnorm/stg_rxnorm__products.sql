-- stg_rxnorm__products.sql

select
	product.rxcui as rxcui
	, product.str as name
	, product.tty as tty
    , case
        when brand_product.rxcui is not null then brand_product.clinical_product_rxcui
        else product.rxcui
        end as clinical_product_rxcui
	, case
        when product.suppress = 'N' then true
        else false
        end as active
	, case 
        when product.cvf = '4096' then true 
        else false
        end as prescribable
from {{ source('rxnorm', 'rxnorm_rxnconso') }} product
left join {{ ref('stg_rxnorm__brand_products') }} brand_product
    on product.rxcui = brand_product.rxcui
where product.tty in('SCD', 'GPCK', 'SBD', 'BPCK')
	and product.sab = 'RXNORM'

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
