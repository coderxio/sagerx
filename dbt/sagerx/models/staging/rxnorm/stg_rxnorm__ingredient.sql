-- stg_rxnorm__ingredient.sql

select
	ingredient.rxcui rxcui
	, ingredient.str name
	, ingredient.tty tty
	, case when ingredient.suppress = 'N' then true else false end as active
	, case when ingredient.cvf = '4096' then true else false end as prescribable
from datasource.rxnorm_rxnconso ingredient
where ingredient.tty in('IN', 'MIN')
	and ingredient.sab = 'RXNORM'
