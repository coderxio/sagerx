select
    rxcui,
    (ndc_item->>'ndc')::json->>0 as ndc,
    ndc_item->>'startdate' as start_date,
    ndc_item->>'enddate' as end_date,
    case when item->>'status' = 'indirect'
        then item->>'rxcui'
        end as indirect_rxcui
from datasource.rxnorm_historical
cross join lateral json_array_elements(ndcs->'historicalndctime') as item
cross join lateral json_array_elements(item->'ndctime') as ndc_item
