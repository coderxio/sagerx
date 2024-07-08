-- stg_rxnorm__clinical_product_component_links.sql

WITH product AS (
SELECT * FROM {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, rxnrel AS (
SELECT * FROM {{ source('rxnorm', 'rxnorm_rxnrel') }} 
)

, product_component AS (
SELECT * FROM {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select distinct
	product.rxcui as clinical_product_rxcui
	, case when product_component.rxcui is null
        then product.rxcui 
        else product_component.rxcui 
        end as clinical_product_component_rxcui
from product
left join rxnrel 
    on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join product_component
    on rxnrel.rxcui1 = product_component.rxcui
    and product_component.tty = 'SCD'
    and product_component.sab = 'RXNORM'
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM'
