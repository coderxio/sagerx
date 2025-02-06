-- stg_ashp__current_drug_shortages_ndcs.sql

with 

ashp_shortage_list as (

    select
        detail_url,
        -- Prepare description by removing any commas inside of parentheses
        regexp_replace(ndc_description, '\(([^)]*),([^)]*)\)', '(\1\2)', 'g') as ndc_description,
        ndc_type
    from {{ source('ashp', 'ashp_shortage_list_ndcs') }}

),

current_drug_shortages_ndcs as (

    select
        split_part(detail_url, '=', 2)::int as id,
        split_part(ndc_description, ',', 1) as product,
        split_part(ndc_description, ',', 2) as manufacturer,
        -- Split NDC description by commas and keep array items 3 through n-1
        array_to_string((string_to_array(ndc_description, ','))[3:array_upper(string_to_array(ndc_description, ','), 1)-1], ',') as description,
        -- Get NDC using regular expression
        replace((regexp_match(ndc_description, '\d{5}\-\d{4}\-\d{2}'))[1], '-', '') as ndc_11,
        ndc_type
    from ashp_shortage_list

)

select
    *
from current_drug_shortages_ndcs
