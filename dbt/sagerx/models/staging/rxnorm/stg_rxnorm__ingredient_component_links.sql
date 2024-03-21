-- stg_rxnorm__ingredient_component_links.sql

with cte as (
	select
		rxnrel.rxcui2 as ingredient_rxcui
		, ingredient_component.rxcui as ingredient_component_rxcui
		, ingredient_component.str as ingredient_component_name
		, ingredient_component.tty as ingredient_component_tty
	from
		sagerx_lake.rxnorm_rxnrel rxnrel
	inner join sagerx_lake.rxnorm_rxnconso ingredient_component
		on rxnrel.rxcui1 = ingredient_component.rxcui
	where rxnrel.rela = 'has_part'
		and ingredient_component.tty = 'IN'
		and ingredient_component.sab = 'RXNORM'
)

select distinct
	ingredient.rxcui as ingredient_rxcui
	, case when cte.ingredient_component_rxcui is null
        then ingredient.rxcui
        else cte.ingredient_component_rxcui
        end as ingredient_component_rxcui
from sagerx_lake.rxnorm_rxnconso ingredient
left join cte on ingredient.rxcui = cte.ingredient_rxcui
where ingredient.tty in('IN', 'MIN')
	and ingredient.sab = 'RXNORM'
