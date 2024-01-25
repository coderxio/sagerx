-- stg_rxnorm_historical__ndcs.sql

select
    rxcui,
    (ndc_item->>'ndc')::json->>0 as ndc,
    ndc_item->>'startDate' as start_date,
    ndc_item->>'endDate' as end_date,
    case when item->>'status' = 'indirect'
        then item->>'rxcui'
        end as indirect_rxcui
from sagerx_lake.rxnorm_historical
    cross join lateral json_array_elements(ndcs->'historicalNdcTime') as item
    cross join lateral json_array_elements(item->'ndcTime') as ndc_item
