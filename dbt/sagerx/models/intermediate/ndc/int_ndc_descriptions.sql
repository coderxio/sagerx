-- int_ndc_descriptions.sql

with 

most_recent_rxnorm_historical_ndcs as (

    select
        *
    from {{ ref('stg_rxnorm_historical__ndcs') }}
    where start_date_line = 1

)

select
    *
from most_recent_rxnorm_historical_ndcs
