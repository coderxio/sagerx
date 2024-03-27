-- stg_rxnorm__precise_ingredients.sql

select
	ingredient.rxcui rxcui
	, ingredient.str name
	, ingredient.tty tty
	, case when ingredient.suppress = 'N' then true else false end as active
	, case when ingredient.cvf = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnconso ingredient
where ingredient.tty = 'PIN'
	and ingredient.sab = 'RXNORM'
