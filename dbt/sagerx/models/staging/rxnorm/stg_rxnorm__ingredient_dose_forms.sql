-- stg_rxnorm__ingredient_dose_forms.sql

select
	ingredient_dose_form.rxcui rxcui
	, ingredient_dose_form.str name
	, ingredient_dose_form.tty tty
	, case when ingredient_dose_form.suppress = 'N' then true else false end as active
	, case when ingredient_dose_form.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnconso ingredient_dose_form
where ingredient_dose_form.tty in ('SCDF', 'SBDF')
	and ingredient_dose_form.sab = 'RXNORM'
