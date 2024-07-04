-- stg_rxnorm__ingredient_component_links.sql

with cte as (
	select * from {{ ref('stg_rxnorm__common_ingredient_component') }}
)

select distinct
	cte.rxcui as ingredient_rxcui
	, case when cte.ingredient_component_rxcui is null
        then cte.rxcui
        else cte.ingredient_component_rxcui
        end as ingredient_component_rxcui
from cte 