-- stg_rxnorm__ndcs.sql
with rxnsat as (
	select * from {{ source('rxnorm', 'rxnorm_rxnsat') }} 
)

, product as (
select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, rxnrel as (
select * from {{ source('rxnorm', 'rxnorm_rxnrel') }} 
)

, clinical_product as (
select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select rxnsat.atv as ndc
	,case when product.tty in ('BPCK','SBD') then clinical_product.rxcui
		else rxnsat.rxcui end as clinical_product_rxcui		
	,case when product.tty in ('BPCK','SBD') then rxnsat.rxcui
		else null end as brand_product_rxcui
	, case when rxnsat.suppress = 'N' then true else false end as active
	, case when rxnsat.cvf = '4096' then true else false end as prescribable
from rxnsat
inner join product 
	on rxnsat.rxaui = product.rxaui
left join rxnrel 
	on rxnsat.rxcui = rxnrel.rxcui2 
	and rela = 'tradename_of' 
	and product.tty in ('BPCK','SBD')
left join clinical_product
	on rxnrel.rxcui1 = clinical_product.rxcui
	and clinical_product.tty in ('SCD','GPCK')
	and clinical_product.sab = 'RXNORM'
where rxnsat.atn = 'NDC'
	and rxnsat.sab in ('ATC', 'CVX', 'DRUGBANK', 'MSH', 'MTHCMSFRF', 'MTHSPL', 'RXNORM', 'USP', 'VANDF')
	and product.tty in ('SCD','SBD','GPCK','BPCK')
	and product.sab = 'RXNORM'
