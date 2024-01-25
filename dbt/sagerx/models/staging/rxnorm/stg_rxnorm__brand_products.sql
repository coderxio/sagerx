-- stg_rxnorm__brand_products.sql

select
	product.rxcui as rxcui
	, product.str as name
	, product.tty as tty
	, clinical_product.rxcui as clinical_product_rxcui
	, case when product.suppress = 'N' then true else false end as active
	, case when product.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnconso product
left join sagerx_lake.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'tradename_of'
left join sagerx_lake.rxnorm_rxnconso clinical_product
	on rxnrel.rxcui1 = clinical_product.rxcui
	and clinical_product.tty in ('SCD', 'GPCK')
	and clinical_product.sab = 'RXNORM'
where product.tty in('SBD', 'BPCK')
	and product.sab = 'RXNORM'
