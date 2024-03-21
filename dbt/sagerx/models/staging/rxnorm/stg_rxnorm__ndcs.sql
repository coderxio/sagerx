-- stg_rxnorm__ndcs.sql

select rxnsat.atv as ndc
	,case when product.tty in ('BPCK','SBD') then clinical_product.rxcui
		else rxnsat.rxcui end as clinical_product_rxcui		
	,case when product.tty in ('BPCK','SBD') then rxnsat.rxcui
		else null end as brand_product_rxcui
	, case when rxnsat.suppress = 'N' then true else false end as active
	, case when rxnsat.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnsat rxnsat
	inner join sagerx_lake.rxnorm_rxnconso product on rxnsat.rxaui = product.rxaui
	left join sagerx_lake.rxnorm_rxnrel rxnrel on rxnsat.rxcui = rxnrel.rxcui2 and rela = 'tradename_of' and product.tty in ('BPCK','SBD')
	left join sagerx_lake.rxnorm_rxnconso clinical_product
		on rxnrel.rxcui1 = clinical_product.rxcui
		and clinical_product.tty in ('SCD','GPCK')
		and clinical_product.sab = 'RXNORM'
where rxnsat.atn = 'NDC'
	and rxnsat.sab in ('ATC', 'CVX', 'DRUGBANK', 'MSH', 'MTHCMSFRF', 'MTHSPL', 'RXNORM', 'USP', 'VANDF')
	and product.tty in ('SCD','SBD','GPCK','BPCK')
	and product.sab = 'RXNORM'
