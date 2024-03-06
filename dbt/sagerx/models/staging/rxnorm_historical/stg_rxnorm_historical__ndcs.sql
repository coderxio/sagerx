-- stg_rxnorm_historical__ndcs.sql

select
    rxcui,
    ndc,
    "startDate" as start_date,
    "endDate" as end_date,
    case when status = 'indirect'
        then rxcui
        end as indirect_rxcui
from datasource.rxnorm_historical
