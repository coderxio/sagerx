-- stg_rxnorm__dose_forms.sql

with dose_form as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select
	dose_form.rxcui rxcui
	, dose_form.str name
	, dose_form.tty tty
	, case when dose_form.suppress = 'N' then true else false end as active
	, case when dose_form.cvf = '4096' then true else false end as prescribable
from dose_form
where dose_form.tty = 'DF'
	and dose_form.sab = 'RXNORM'
