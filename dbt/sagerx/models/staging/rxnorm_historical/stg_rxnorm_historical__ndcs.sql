-- stg_rxnorm_historical__ndcs.sql

with

rxnorm_historical as (

    select
        rxcui,
        (ndc_item->>'ndc')::json->>0 as ndc,
        ndc_item->>'startDate' as start_date,
        ndc_item->>'endDate' as end_date,
        case when item->>'status' = 'indirect'
            then item->>'rxcui'
            end as indirect_rxcui
    from datasource.rxnorm_historical
        cross join lateral json_array_elements(ndcs->'historicalNdcTime') as item
        cross join lateral json_array_elements(item->'ndcTime') as ndc_item

)

select
    ndc
	, row_number() over (partition by ndc order by start_date desc) as start_date_line
    , start_date
    , end_date
    , indirect_rxcui
from rxnorm_historical
