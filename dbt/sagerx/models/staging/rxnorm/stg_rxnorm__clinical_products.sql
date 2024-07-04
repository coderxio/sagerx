-- stg_rxnorm__clinical_products.sql

with product as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select
	product.rxcui as rxcui
	, product.str as name
	, product.tty as tty
	, case when product.suppress = 'N' then true else false end as active
	, case when product.cvf = '4096' then true else false end as prescribable
from product
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM'
