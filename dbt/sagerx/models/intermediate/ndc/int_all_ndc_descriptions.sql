-- int_all_ndc_descriptions.sql

with 

rxnorm_ndcs as (

    select
        ndc
        , product_rxcui as rxcui
        , product_name as name
    from {{ ref('int_rxnorm_ndcs_to_products') }}

)

, rxnorm_product_rxcuis as (

    select * from {{ source('rxnorm', 'rxnorm_rxnconso') }}
    where sab = 'RXNORM'
        and tty in ('SCD', 'SBD', 'GPCK', 'BPCK')

)

/*
NOTE: do we really only want the most recent historical NDCs?
maybe should mash rxnorm_historical_ndcs up against rxnorm_ndcs
and somehow filter out any parts that are wrong?
*/
, most_recent_rxnorm_historical_ndcs as (

    select
        rxnorm_historical_most_recent_ndcs.ndc::varchar
        , rxnorm_historical_most_recent_ndcs.rxcui::varchar
        , rxnorm_product_rxcuis.str as name
    from {{ ref('stg_rxnorm_historical__most_recent_ndcs') }} rxnorm_historical_most_recent_ndcs
    left join rxnorm_product_rxcuis
        on rxnorm_product_rxcuis.rxcui = rxnorm_historical_most_recent_ndcs.rxcui::varchar

)

, fda_ndc_ndcs as (

    select
        ndc11 as ndc
        , null as rxcui
        , concat(
            nonproprietaryname
            , ' '
            , active_numerator_strength
            , ' '
            , active_ingred_unit
            , ' '
            , lower(dosageformname)
            , case when proprietaryname is not null then concat(
                ' ['
                , proprietaryname
                , case when proprietarynamesuffix is not null then concat(
                    ' '
                    , proprietarynamesuffix
                    ) else '' end
                , ']'
                ) else '' end
            ) as name
    from {{ ref('stg_fda_ndc__ndcs') }}

)

, fda_unfinished_ndcs as (

    select
        ndc11 as ndc
        , null as rxcui
        , nonproprietaryname as name
    from {{ ref('stg_fda_unfinished__ndcs') }}

)

, fda_excluded_ndcs as (

    select
        ndc11 as ndc
        , null as rxcui
        , concat(proprietaryname, ' ', proprietarynamesuffix, ' (', nonproprietaryname, ')') as name
    from {{ ref('stg_fda_excluded__ndcs') }}

)

, all_rxnorm_ndcs as (

    select * from rxnorm_ndcs

    union
    
    select * from most_recent_rxnorm_historical_ndcs

)

, all_fda_ndcs as (

    select * from fda_ndc_ndcs
    
    union
    
    select * from fda_unfinished_ndcs
    
    union
    
    select * from fda_excluded_ndcs

)

, all_fda_ndcs_not_in_rxnorm as (

    select
        all_fda_ndcs.*
    from all_fda_ndcs
    left join all_rxnorm_ndcs
        on all_rxnorm_ndcs.ndc = all_fda_ndcs.ndc
    where all_rxnorm_ndcs.ndc is null

)

, all_ndc_descriptions as (

    select * from all_rxnorm_ndcs

    union

    select * from all_fda_ndcs_not_in_rxnorm

)

select * from all_ndc_descriptions
where ndc is not null
