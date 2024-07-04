-- stg_rxnorm__common_ingredient_component.sql

with rxnrel as (
    select * from {{ source('rxnorm', 'rxnorm_rxnrel') }}
)

, ingredient_component as (
    select * from {{ source('rxnorm', 'rxnorm_rxnconso') }}
)

, cte as (
    select
        rxnrel.rxcui2 as ingredient_rxcui
        , ingredient_component.rxcui as rxcui
        , ingredient_component.str as name
        , ingredient_component.tty as tty
        , ingredient_component.suppress
        , ingredient_component.cvf
    from rxnrel
    inner join ingredient_component
        on rxnrel.rxcui1 = ingredient_component.rxcui
    where rxnrel.rela = 'has_part'
        and ingredient_component.tty = 'IN'
        and ingredient_component.sab = 'RXNORM'
)

select
    ingredient_component.*
    , cte.ingredient_rxcui
    , cte.rxcui as ingredient_component_rxcui
    , cte.name as ingredient_component_name
    , cte.tty as ingredient_component_tty
    , cte.suppress as ingredient_component_suppress
    , cte.cvf as ingredient_component_cvf
from ingredient_component
left join cte 
    on ingredient_component.rxcui = cte.ingredient_rxcui
where ingredient_component.tty in('IN', 'MIN')
	and ingredient_component.sab = 'RXNORM'