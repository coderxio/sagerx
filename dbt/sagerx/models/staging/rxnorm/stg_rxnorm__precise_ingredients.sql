-- stg_rxnorm__precise_ingredients.sql

WITH ingredient AS (
	SELECT * FROM {{ source('rxnorm', 'rxnorm_rxnconso') }}
)

select
	ingredient.rxcui rxcui
	, ingredient.str name
	, ingredient.tty tty
	, case when ingredient.suppress = 'N' then true else false end as active
	, case when ingredient.cvf = '4096' then true else false end as prescribable
from ingredient
where ingredient.tty = 'PIN'
	and ingredient.sab = 'RXNORM'
