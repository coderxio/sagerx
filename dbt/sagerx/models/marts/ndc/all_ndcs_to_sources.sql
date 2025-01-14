-- stg_ndc_all_ndcs_to_sources.sql

with rxnorm_historical_ndcs as
(
    -- NOTE: likely need to pick the most recent NDC
    -- instead of just distinct
    -- and pull in the other columns too possibly
    select distinct ndc
    from {{ ref('stg_rxnorm_historical__ndcs') }}
)

, rxnorm_ndcs as
(
    select distinct ndc
    from {{ ref('stg_rxnorm__ndcs') }}
)

, fda_ndc_ndcs as
(
    select distinct ndc11 as ndc
    from {{ ref('stg_fda_ndc__ndcs') }}
)

, fda_excluded_ndcs as
(
    select distinct ndc11 as ndc
    from {{ ref('stg_fda_excluded__ndcs') }}
)

, fda_unfinished_ndcs as
(
    select distinct ndc11 as ndc
    from {{ ref('stg_fda_unfinished__ndcs') }}
)

, all_distinct_ndcs as
(
    select ndc from rxnorm_historical_ndcs
    union
    select ndc from rxnorm_ndcs
    union
    select ndc from fda_ndc_ndcs
    union
    select ndc from fda_excluded_ndcs
    union
    select ndc from fda_unfinished_ndcs
)

, all_ndcs_to_sources as (
    select
        all_distinct_ndcs.ndc
        , case when rxnorm_historical_ndcs.ndc is not null
            then 1 
            else 0
            end as rxnorm_historical_ndcs
        , case when rxnorm_ndcs.ndc is not null
            then 1 
            else 0
            end as rxnorm_ndcs
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
    left join rxnorm_ndcs
        on rxnorm_ndcs.ndc = all_distinct_ndcs.ndc
    left join fda_ndc_ndcs
        on fda_ndc_ndcs.ndc = all_distinct_ndcs.ndc
    left join fda_excluded_ndcs
        on fda_excluded_ndcs.ndc = all_distinct_ndcs.ndc
    left join fda_unfinished_ndcs
        on fda_unfinished_ndcs.ndc = all_distinct_ndcs.ndc
)

, all_not_null_ndcs_to_sources as (
    select
        *
    from all_ndcs_to_sources
    where ndc is not null
)

, all_ndc_descriptions as (
    select * from {{ ref('all_ndc_descriptions') }}
)

, all_ndcs_with_descriptions_to_sources as (
    select
        all_not_null_ndcs_to_sources.*
    from all_not_null_ndcs_to_sources
    inner join all_ndc_descriptions
        on all_ndc_descriptions.ndc = all_not_null_ndcs_to_sources.ndc
)

select * from all_ndcs_with_descriptions_to_sources
