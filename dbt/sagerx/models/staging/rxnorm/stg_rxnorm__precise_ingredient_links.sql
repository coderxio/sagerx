-- stg_rxnorm__precise_ingredient_links.sql

select distinct
	ingredient_strength.rxcui as ingredient_strength_rxcui
	, precise_ingredient.rxcui as precise_ingredient_rxcui
from sagerx_lake.rxnorm_rxnconso precise_ingredient
inner join sagerx_lake.rxnorm_rxnrel precise_ingredient_of
    on precise_ingredient_of.rxcui2 = precise_ingredient.rxcui
    and precise_ingredient_of.rela = 'precise_ingredient_of'
inner join sagerx_lake.rxnorm_rxnconso ingredient_strength
	on ingredient_strength.rxcui = precise_ingredient_of.rxcui1
	and ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM'
where precise_ingredient.tty = 'PIN'
	and precise_ingredient.sab = 'RXNORM'
