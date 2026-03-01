-- int_umls_multiple_ingredients_to_crosswalk_codes.sql

with

multiple_ingredients as (

    select
        *
    from {{ ref('stg_rxclass__rxclass') }}
    where tty = 'MIN'

),

crosswalk_codes as (

    select
        *
    from {{ ref('stg_umls__crosswalk_codes') }}
)

select
    multiple_ingredients.rxcui as multiple_ingredient_rxcui,
    multiple_ingredients.name as multiple_ingredient_name,
    multiple_ingredients.tty as multiple_ingredient_tty,
    multiple_ingredients.rela,
    multiple_ingredients.class_id,
    multiple_ingredients.class_name,
    multiple_ingredients.class_type,
    multiple_ingredients.rela_source,
    crosswalk_codes.*
from multiple_ingredients
inner join crosswalk_codes
    on crosswalk_codes.from_code = multiple_ingredients.class_id
