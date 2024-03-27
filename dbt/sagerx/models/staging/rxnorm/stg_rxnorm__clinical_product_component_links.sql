-- stg_rxnorm__clinical_product_component_links.sql

select distinct
	product.rxcui as clinical_product_rxcui
	, case when product_component.rxcui is null
        then product.rxcui 
        else product_component.rxcui 
        end as clinical_product_component_rxcui
from sagerx_lake.rxnorm_rxnconso product
left join sagerx_lake.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join sagerx_lake.rxnorm_rxnconso product_component
    on rxnrel.rxcui1 = product_component.rxcui
    and product_component.tty = 'SCD'
    and product_component.sab = 'RXNORM'
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM'
