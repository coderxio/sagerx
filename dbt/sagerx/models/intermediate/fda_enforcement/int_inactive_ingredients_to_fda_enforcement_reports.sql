with

regex_ndcs as (

    select * from {{ ref('stg_fda_enforcement__regex_ndcs') }}

),

json_ndcs as (

    select * from {{ ref('stg_fda_enforcement__json_ndcs') }}

),

enf as (
    -- UNION RegEx NDCs from description and JSON NDCs from OpenFDA column
    -- NOTE: UNION removes duplicates by default, which is what we want here
    select * from (
        -- first get only the NDCs from combined RegEx and JSON enforcement report data
        select 
            recall_number
            , ndc11
            , ndc9
        from regex_ndcs
        
        union
        
        select
            recall_number
            , ndc11
            , ndc9
        from json_ndcs
    ) enf_ndcs
    -- then, join NDCs with application numbers, where they exist from JSON data
    left join (
        -- get distinct NDC11 -> application numbers so as to not blow up granularity
        select
            ndc11
            , app_num
        from json_ndcs
        group by
            ndc11
            , app_num
    ) json_app_num
        on enf_ndcs.ndc11 = json_app_num.ndc11
),

ndc as (

    select * from {{ ref('int_rxnorm_ndcs_to_products') }}

),

ing as (

    select * from {{ ref('int_rxnorm_clinical_products_to_ingredients') }}

),

spl as (

    select * from {{ ref('int_mthspl_products_to_inactive_ingredients') }}

),

fda as (

    select * from {{ ref('stg_fda_ndc__ndcs') }}

)

select distinct
    ndc.ndc as ndc11
    , left(ndc.ndc, 9) as ndc9
    , ndc.product_rxcui
    , ndc.product_name
    , ndc.product_tty
    , ndc.clinical_product_rxcui
    , ndc.clinical_product_name
    , ndc.clinical_product_tty
    , ing.ingredient_rxcui as active_ingredient_rxcui
    , ing.ingredient_name as active_ingredient_name
    , ing.ingredient_tty as active_ingredient_tty
    -- if FDA NDC Directory has an application number, use that...
    -- otherwise, use the application number from the enforcement report JSON
    , coalesce(fda.applicationnumber, enf.app_num) as application_number
    , fda.labelername as labeler
    , enf.recall_number
    , spl.inactive_ingredient_unii
    , spl.inactive_ingredient_rxcui
    , spl.inactive_ingredient_name
    , spl.inactive_ingredient_tty
-- RxNorm NDCs that have one of the ingredients in the WHERE clause
from ndc
-- clinical product and related active ingredients for each NDC
left join ing
    on ing.clinical_product_rxcui = ndc.clinical_product_rxcui
-- FDA enforcement reports (UNION of RegEx and JSON) joined on NDC9
left join enf
    on enf.ndc9 = left(ndc.ndc, 9)
-- DailyMed SPL inactive ingredients joined on NDC9
left join spl
    on spl.ndc9 = left(ndc.ndc, 9)
-- FDA NDC Directory joined on NDC11 to pull in application number
left join fda
    on fda.ndc11 = ndc.ndc
