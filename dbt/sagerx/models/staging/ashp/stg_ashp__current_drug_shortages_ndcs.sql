-- stg_ashp__current_drug_shortages_ndcs.sql

with 

ashp_shortage_list as (

    select * from {{ source('ashp', 'ashp_shortage_list_ndcs') }}

),

current_drug_shortages_ndcs as (

    select
        split_part(detail_url, '=', 2)::int as id,
        replace(ndc, '-', '') as ndc_11,
        ndc_type
    from ashp_shortage_list

)

select
    *
from current_drug_shortages_ndcs
