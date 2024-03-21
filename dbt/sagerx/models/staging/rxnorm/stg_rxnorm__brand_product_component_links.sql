-- stg_rxnorm__brand_product_component_links.sql

select distinct
	product.rxcui as brand_product_rxcui
	, case when product_component.rxcui is null
        then product.rxcui
        else product_component.rxcui
        end as brand_product_component_rxcui
from sagerx_lake.rxnorm_rxnconso product
left join sagerx_lake.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join sagerx_lake.rxnorm_rxnconso product_component
	on rxnrel.rxcui1 = product_component.rxcui
	and product_component.tty in ('SBD', 'SCD') -- NOTE: BPCKs can contain SBDs AND SCDs
	and product_component.sab = 'RXNORM'
where product.tty in ('SBD', 'BPCK')
	and product.sab = 'RXNORM'
