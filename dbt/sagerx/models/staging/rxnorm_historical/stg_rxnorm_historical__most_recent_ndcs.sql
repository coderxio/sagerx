-- stg_rxnorm_historical__most_recent_ndcs.sql

with 

rxnorm_historical_ndcs as
(
    select * from {{ ref('stg_rxnorm_historical__ndcs') }}
)

/*
NOTE: we do this grouping and ranking to avoid NDCs that
relate to RXCUIs that have been remapped to multiple RXCUIs
- see issue #265 for more details
*/
, grouped_and_ranked_rxnorm_historical_ndcs as
(
    
    select
        ndc
        , end_date
        , row_number() over (partition by ndc order by end_date desc) as end_date_line
        , count(rxcui) as rxcui_count
    from rxnorm_historical_ndcs
    group by ndc, end_date
    order by count(rxcui) desc

)

select
    rxnorm_historical_ndcs.*
from grouped_and_ranked_rxnorm_historical_ndcs
inner join rxnorm_historical_ndcs
    on rxnorm_historical_ndcs.ndc = grouped_and_ranked_rxnorm_historical_ndcs.ndc
    and rxnorm_historical_ndcs.end_date = grouped_and_ranked_rxnorm_historical_ndcs.end_date
where rxcui_count = 1 -- only NDCs that are associated with one RXCUI per end_date
	and end_date_line = 1 -- only NDCs that are most recently associated with an RXCUI
