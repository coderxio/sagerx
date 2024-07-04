-- stg_rxnorm__brand_products.sql

with product as (
	select * from  {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, rxnrel as (
	select * from  {{ source('rxnorm', 'rxnorm_rxnrel') }} 
)

, product_component as (
	select * from  {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select
	product.rxcui as rxcui
	, product.str as name
	, product.tty as tty
	, product_component.rxcui as clinical_product_rxcui
	, case when product.suppress = 'N' then true else false end as active
	, case when product.cvf = '4096' then true else false end as prescribable
from product
left join rxnrel 
	on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'tradename_of'
left join product_component
	on rxnrel.rxcui1 = product_component.rxcui
	and product_component.tty in ('SCD', 'GPCK')
	and product_component.sab = 'RXNORM'
where product.tty in('SBD', 'BPCK')
	and product.sab = 'RXNORM'
