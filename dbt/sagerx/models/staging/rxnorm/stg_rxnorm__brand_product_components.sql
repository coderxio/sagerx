-- stg_rxnorm__brand_product_components.sql

select distinct
	case when product.tty = 'SBD' then product.rxcui else product_component.rxcui end rxcui
	, case when product.tty = 'SBD' then product.str else product_component.str end name
	, case when product.tty = 'SBD' then product.tty else product_component.tty end tty
	, case when product_component.tty = 'SCD' then product_component.rxcui else rxnrel_scd.rxcui1 end clinical_product_component_rxcui
	, rxnrel_bn.rxcui1 as brand_rxcui
	, case when 
            case when product.tty = 'SBD'
            then product.suppress
            else product_component.suppress
            end = 'N' 
        then true 
        else false
        end as active
	, case when 
            case when product.tty = 'SBD'
            then product.cvf
            else product_component.cvf 
            end = '4096' 
        then true
        else false
        end as prescribable
from sagerx_lake.rxnorm_rxnconso product
left join sagerx_lake.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join sagerx_lake.rxnorm_rxnconso product_component
	on rxnrel.rxcui1 = product_component.rxcui
	and product_component.tty in ('SBD', 'SCD') -- NOTE: BPCKs can contain SBDs AND SCDs
	and product_component.sab = 'RXNORM'
left join sagerx_lake.rxnorm_rxnrel rxnrel_scd 
	on rxnrel_scd.rxcui2 = case when product_component.rxcui is null then product.rxcui else product_component.rxcui end 
	and rxnrel_scd.rela = 'tradename_of' -- rxnrel_scd.rxcui1 = clinical_product_component_rxcui
left join sagerx_lake.rxnorm_rxnrel rxnrel_bn 
	on rxnrel_bn.rxcui2 = case when product_component.rxcui is null then product.rxcui else product_component.rxcui end 
	and rxnrel_bn.rela = 'has_ingredient' -- rxnrel_bn.rxcui1 = brand_rxcui
where product.tty in ('SBD', 'BPCK')
	and product.sab = 'RXNORM'
