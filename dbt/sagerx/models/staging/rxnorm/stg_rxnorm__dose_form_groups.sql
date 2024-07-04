-- stg_rxnorm__dose_form_groups.sql

with dose_form_group as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select
	dose_form_group.rxcui rxcui
	, dose_form_group.str name
	, dose_form_group.tty tty
	, case when dose_form_group.suppress = 'N' then true else false end as active
	, case when dose_form_group.cvf = '4096' then true else false end as prescribable
from dose_form_group
where dose_form_group.tty = 'DFG'
	and dose_form_group.sab = 'RXNORM'
