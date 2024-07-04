-- stg_rxnorm__ingredient_components.sql

with cte as (
	select * from {{ ref('stg_rxnorm__common_ingredient_component') }}
)

select distinct
	case when cte.ingredient_component_rxcui is null then cte.rxcui 
		else cte.ingredient_component_rxcui 
		end rxcui
	, case when cte.ingredient_component_name is null then cte.str 
		else cte.ingredient_component_name 
		end name
	, case when cte.ingredient_component_tty is null then cte.tty 
		else cte.ingredient_component_tty 
		end tty
	, case when 
		case when cte.ingredient_component_rxcui is null then cte.suppress 
			else cte.ingredient_component_suppress end = 'N' then true else false 
			end as active
	, case when 
		case when cte.ingredient_component_rxcui is null then cte.cvf 
		else cte.ingredient_component_cvf end = '4096' then true else false 
		end as prescribable
from cte 
