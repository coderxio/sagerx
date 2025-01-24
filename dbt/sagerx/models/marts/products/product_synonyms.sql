with

rxnorm_synonyms as (

    select
        str as synonym,
        rxcui as product_rxcui,
        'RXNORM' as source
    from {{ source('rxnorm', 'rxnorm_rxnconso') }}
    where sab = 'RXNORM'
    and tty in('PSN', 'SY', 'TMSY', 'ET')

),

nadac_synonyms as (

    select distinct
        ndc_description as synonym,
        product_rxcui,
        'NADAC' as source
    from {{ source('nadac', 'nadac') }} n
    left join {{ ref('int_rxnorm_ndcs_to_products') }} r
        on r.ndc = n.ndc
    where r.product_rxcui is not null

),

fda_synonyms as (

    select distinct
        trim(concat(
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
            )) as synonym,
            product_rxcui,
            'FDA' as source
    from sagerx_dev.stg_fda_ndc__ndcs f
    left join sagerx_dev.int_rxnorm_ndcs_to_products r
        on r.ndc = f.ndc11
    where r.product_rxcui is not null

),

all_synonyms as (
    
    select * from rxnorm_synonyms

    union

    select * from nadac_synonyms

    union

    select * from fda_synonyms

),

rxnorm_products as (

    select * from {{ ref('stg_rxnorm__products') }}

),

prescribable_product_synonyms as (

    select
        all_synonyms.*
    from all_synonyms
    inner join rxnorm_products
        on rxnorm_products.rxcui = all_synonyms.product_rxcui
    where rxnorm_products.prescribable = true

)

select * from prescribable_product_synonyms
