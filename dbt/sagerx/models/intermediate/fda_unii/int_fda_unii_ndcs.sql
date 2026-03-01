-- int_fda_unii_ndcs.sql

with

inactive_ingredients as (
    
    select
        *
    from {{ ref('int_mthspl_products_to_inactive_ingredients') }}

),

fda_unii as (

    select
        *
    from {{ ref('int_fda_unii') }}

),

ndc_detail as (

    select
        *
    from inactive_ingredients iact
    left join fda_unii
        on iact.inactive_ingredient_unii = fda_unii.unii

),

ndc_summary as (

    select
        ndc9,
        ndc,
        product_rxcui,
        product_name,
        product_tty,
        bool_or(is_gluten) as has_gluten,
        sum(gluten_count) as gluten_count,
        bool_or(is_dye) as has_dye,
        sum(dye_count) as dye_count
    from ndc_detail
    group by
        ndc9,
        ndc,
        product_rxcui,
        product_name,
        product_tty

),

ndc9_to_ndc11 as (

    select
        left(ndc11, 9) as ndc9,
        ndc11
    from {{ ref('int_rxnorm_all_ndcs_to_product_rxcuis') }}
    where left(ndc11, 9) in (select distinct ndc9 from ndc_summary)

),

ndc_summary_with_ndc11 as (

    select
        ndc9_to_ndc11.ndc11,
        ndc_summary.*
    from ndc_summary
    left join ndc9_to_ndc11
        on ndc_summary.ndc9 = ndc9_to_ndc11.ndc9

)

select * from ndc_summary_with_ndc11
