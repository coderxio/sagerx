-- stg_rxnorm__dose_form_group_links.sql

with rxnrel as (
	select * from {{ source('rxnorm', 'rxnorm_rxnrel') }} 
)

, dose_form as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select distinct
	dose_form.rxcui dose_form_rxcui
	, rxnrel.rxcui1 dose_form_group_rxcui
from dose_form
inner join rxnrel
	on rxnrel.rxcui2 = dose_form.rxcui
	and rxnrel.rela = 'isa'
	and rxnrel.sab = 'RXNORM'
where dose_form.tty = 'DF'
	and dose_form.sab = 'RXNORM'
