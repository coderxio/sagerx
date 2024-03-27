-- stg_rxnorm__dose_form_group_links.sql

select distinct
	dose_form.rxcui dose_form_rxcui
	, rxnrel.rxcui1 dose_form_group_rxcui
from sagerx_lake.rxnorm_rxnconso dose_form
inner join sagerx_lake.rxnorm_rxnrel rxnrel
	on rxnrel.rxcui2 = dose_form.rxcui
	and rxnrel.rela = 'isa'
	and rxnrel.sab = 'RXNORM'
where dose_form.tty = 'DF'
	and dose_form.sab = 'RXNORM'
