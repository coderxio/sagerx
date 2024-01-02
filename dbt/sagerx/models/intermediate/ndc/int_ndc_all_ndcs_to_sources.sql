-- stg_ndc_all_ndcs_to_sources.sql

with rxnorm_historical_ndcs as
(
    -- NOTE: likely need to pick the most recent NDC
    -- instead of just distinct
    -- and pull in the other columns too possibly
    select distinct ndc
    from {{ ref('stg_rxnorm_historical__ndcs') }}
)

, rxnorm_all_ndcs as
(
    select distinct ndc11 as ndc
    from {{ ref('stg_rxnorm__all_ndcs') }}
)

, fda_ndc_ndcs as
(
    select distinct ndc11 as ndc
    from {{ ref('stg_fda_ndc__ndc') }}
)

, fda_excluded_ndcs as
(
    select distinct ndc11 as ndc
    from staging.fda_excluded
)

, fda_unfinished_ndcs as
(
    select distinct ndc11 as ndc
    from staging.fda_unfinished
)

, all_distinct_ndcs as
(
    select ndc from rxnorm_historical_ndcs
    union
    select ndc from rxnorm_all_ndcs
    union
    select ndc from fda_ndc_ndcs
    union
    select ndc from fda_excluded_ndcs
    union
    select ndc from fda_unfinished_ndcs
)

select
    all_distinct_ndcs.ndc
    , case when rxnorm_historical_ndcs.ndc is not null
        then 1 
        else 0
        end as rxnorm_historical_ndcs
    , case when rxnorm_all_ndcs.ndc is not null
        then 1 
        else 0
        end as rxnorm_all_ndcs
    , case when fda_ndc_ndcs.ndc is not null
        then 1 
        else 0
        end as fda_ndc_ndcs
    , case when fda_excluded_ndcs.ndc is not null
        then 1 
        else 0
        end as fda_excluded_ndcs
    , case when fda_unfinished_ndcs.ndc is not null
        then 1 
        else 0
        end as fda_unfinished_ndcs
from all_distinct_ndcs
left join rxnorm_historical_ndcs
    on rxnorm_historical_ndcs.ndc = all_distinct_ndcs.ndc
left join rxnorm_all_ndcs
    on rxnorm_all_ndcs.ndc = all_distinct_ndcs.ndc
left join fda_ndc_ndcs
    on fda_ndc_ndcs.ndc = all_distinct_ndcs.ndc
left join fda_excluded_ndcs
    on fda_excluded_ndcs.ndc = all_distinct_ndcs.ndc
left join fda_unfinished_ndcs
    on fda_unfinished_ndcs.ndc = all_distinct_ndcs.ndc
