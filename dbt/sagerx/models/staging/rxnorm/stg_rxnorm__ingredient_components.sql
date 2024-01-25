-- stg_rxnorm__ingredient_components.sql

with cte as (
	select
		rxnrel.rxcui2 as ingredient_rxcui
		, ingredient_component.rxcui as rxcui
		, ingredient_component.str as name
		, ingredient_component.tty as tty
		, ingredient_component.suppress
		, ingredient_component.cvf
	from
		sagerx_lake.rxnorm_rxnrel rxnrel
	inner join sagerx_lake.rxnorm_rxnconso ingredient_component
		on rxnrel.rxcui1 = ingredient_component.rxcui
	where rxnrel.rela = 'has_part'
		and ingredient_component.tty = 'IN'
		and ingredient_component.sab = 'RXNORM'
)

select distinct
	case when cte.rxcui is null then ingredient.rxcui else cte.rxcui end rxcui
	, case when cte.name is null then ingredient.str else cte.name end name
	, case when cte.tty is null then ingredient.tty else cte.tty end tty
	, case when 
		case when cte.rxcui is null then ingredient.suppress else cte.suppress end = 'N' then true else false end as active
	, case when 
		case when cte.rxcui is null then ingredient.cvf else cte.cvf end = '4096' then true else false end as prescribable
from sagerx_lake.rxnorm_rxnconso ingredient
left join cte on ingredient.rxcui = cte.ingredient_rxcui
where ingredient.tty in('IN', 'MIN')
	and ingredient.sab = 'RXNORM'
