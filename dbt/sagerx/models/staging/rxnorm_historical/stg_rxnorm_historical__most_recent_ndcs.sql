-- stg_rxnorm_historical__most_recent_ndcs.sql

with

rxnorm_historical as (

    select
        *
        -- NOTE: discovered multiples when using start_date, so switched to end_date
        , row_number() over (partition by ndc order by end_date desc) as end_date_line
    from {{ ref('stg_rxnorm_historical__ndcs') }}

)

select
    *
from rxnorm_historical
where end_date_line = 1
