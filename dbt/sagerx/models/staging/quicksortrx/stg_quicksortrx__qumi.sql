-- stg_quicksortrx__qumi.sql

with

quicksortrx_qumi as (
    
    select
        *
    from {{ source('quicksortrx', 'quicksortrx_qumi') }}

),

final as (

    select
        {{ ndc_to_11('ndc') }} as ndc11,
        *
    from quicksortrx_qumi

)

select * from final
