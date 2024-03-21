-- stg_rxnorm__mthspl_products.sql

select
	product.rxcui as rxcui
	, product.str as name
	, product.tty as tty
	, product.rxaui as rxaui
	, product.code as ndc
	, case when product.suppress = 'N' then true else false end as active
	, case when product.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnconso product
where product.tty = 'DP'
	and product.sab = 'MTHSPL'
