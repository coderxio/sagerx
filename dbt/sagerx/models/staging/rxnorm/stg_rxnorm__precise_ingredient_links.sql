-- stg_rxnorm__precise_ingredient_links.sql

with precise_ingredient as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, precise_ingredient_of as (
	select * from {{ source('rxnorm', 'rxnorm_rxnrel') }} 
)

, ingredient_strength as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

select distinct
	ingredient_strength.rxcui as ingredient_strength_rxcui
	, precise_ingredient.rxcui as precise_ingredient_rxcui
from precise_ingredient
inner join precise_ingredient_of
    on precise_ingredient_of.rxcui2 = precise_ingredient.rxcui
    and precise_ingredient_of.rela = 'precise_ingredient_of'
inner join ingredient_strength
	on ingredient_strength.rxcui = precise_ingredient_of.rxcui1
	and ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM'
where precise_ingredient.tty = 'PIN'
	and precise_ingredient.sab = 'RXNORM'
