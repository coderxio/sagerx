-- all_ndc_descriptions.sql

with 

/*
NOTE: this intermediate table is only NDCs that have
SAB = RXNORM RXCUIs, which limits the total NDC count - 
there are lots of NDCs that don't have RXNORM RXCUIs
that might be good to include in the future, but a lot
seem to be inactive or not prescribable
MTHSPL = 174k+, VANDF = 309k+, CVX = 500+
*/
rxnorm_ndcs as (

    select
        ndc
        , product_rxcui as rxcui
        , product_name as rxnorm_description
    from {{ ref('int_rxnorm_ndcs_to_products') }}

) 

/* 
NOTE: maybe want to make a stg_rxnorm__products table
to replace this weird one-off table
*/
, rxnorm_product_rxcuis as (

    select * from {{ ref('stg_rxnorm__product_rxcuis') }}

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
        , rxnorm_product_rxcuis.str as rxnorm_description
    from {{ ref('stg_rxnorm_historical__most_recent_ndcs') }} rxnorm_historical_most_recent_ndcs
    left join rxnorm_product_rxcuis
        on rxnorm_product_rxcuis.rxcui = rxnorm_historical_most_recent_ndcs.rxcui::varchar

)

, fda_ndc_ndcs as (

    select
        ndc11 as ndc
        , trim(concat(
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
            )) as fda_description
    from {{ ref('stg_fda_ndc__ndcs') }}

)

, fda_unfinished_ndcs as (

    select
        ndc11 as ndc
        , trim(concat(
            nonproprietaryname
            , ' '
            , active_numerator_strength
            , ' '
            , active_ingred_unit
            , ' '
            , lower(dosageformname)
            )) as fda_description
    from {{ ref('stg_fda_unfinished__ndcs') }}

)

, fda_excluded_ndcs as (

    select
        ndc11 as ndc
        , trim(concat(
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
            )) as fda_description
    from {{ ref('stg_fda_excluded__ndcs') }}

)

, all_rxnorm_ndcs as (
    
    select *, 1 as table_rank from rxnorm_ndcs

    union

    select *, 2 as table_rank from most_recent_rxnorm_historical_ndcs

)

, ranked_rxnorm_ndcs as (

    select
        *
        , row_number() over (partition by ndc order by table_rank asc) as row_num
    from all_rxnorm_ndcs

)

, distinct_rxnorm_ndcs as (

    select
        ndc
        , rxcui
        , rxnorm_description
    from ranked_rxnorm_ndcs
    where row_num = 1

)

, all_fda_ndcs as (

    select *, 1 as table_rank from fda_ndc_ndcs
    
    union
    
    select *, 2 as table_rank from fda_excluded_ndcs

    union

    select *, 3 as table_rank from fda_unfinished_ndcs    

)

, ranked_fda_ndcs as (

    select
        *
        , row_number() over (partition by ndc order by table_rank asc) as row_num
    from all_fda_ndcs

)

, distinct_fda_ndcs as (

    select
        ndc
        , fda_description
    from ranked_fda_ndcs
    where row_num = 1

)

, all_ndcs as (

    select ndc from distinct_rxnorm_ndcs

    union
    
    select ndc from distinct_fda_ndcs

)

, all_ndc_descriptions as (

    select
        all_ndcs.ndc
        , rxcui
        , rxnorm_description
        , fda_description
    from all_ndcs
    left join distinct_rxnorm_ndcs
        on distinct_rxnorm_ndcs.ndc = all_ndcs.ndc
    left join distinct_fda_ndcs
        on distinct_fda_ndcs.ndc = all_ndcs.ndc

)

, all_not_null_ndc_descriptions as (

select * from all_ndc_descriptions
where ndc is not null

)

select * from all_not_null_ndc_descriptions
