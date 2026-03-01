-- int_fda_unii.sql

with

gluten_unii_codes as (

    select
        *,
        true as is_true,
        1 as count    
    from {{ ref('fda_unii_excipient_gluten') }}

),

dye_unii_codes as (

    select
        *,
        true as is_true,
        1 as count        
    from {{ ref('fda_unii_excipient_dye') }}

),

fda_unii as (

    select 
        fda_unii.*,
        gluten.is_true as is_gluten,
        gluten.count as gluten_count,
        dye.is_true as is_dye,
        dye.count as dye_count
    from {{ ref('stg_fda_unii__unii_codes') }} fda_unii
    left join gluten_unii_codes gluten
        on fda_unii.unii = gluten.fda_unii_code
    left join dye_unii_codes dye
        on fda_unii.unii = dye.fda_unii_code

)

select * from fda_unii
