-- stg_rxnorm__ingredient_strength_links.sql

select distinct
	product_component.rxcui as clinical_product_component_rxcui
	, ingredient.rxcui as ingredient_component_rxcui
	, ingredient_strength.rxcui as ingredient_strength_rxcui
from sagerx_lake.rxnorm_rxnconso ingredient_strength
inner join sagerx_lake.rxnorm_rxnrel has_ingredient
    on has_ingredient.rxcui2 = ingredient_strength.rxcui
    and has_ingredient.rela = 'has_ingredient'
inner join sagerx_lake.rxnorm_rxnconso ingredient
	on ingredient.rxcui = has_ingredient.rxcui1
	and ingredient.tty = 'IN'
	and ingredient.sab = 'RXNORM'
inner join sagerx_lake.rxnorm_rxnrel constitutes
    on constitutes.rxcui2 = ingredient_strength.rxcui
    and constitutes.rela = 'constitutes'
inner join sagerx_lake.rxnorm_rxnconso product_component
	on product_component.rxcui = constitutes.rxcui1
	and product_component.tty = 'SCD'
	and product_component.sab = 'RXNORM'
where ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM'
