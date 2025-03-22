-- int_umls_precise_ingredients_to_crosswalk_codes.sql

with

precise_ingredients as (

    select
        *
    from {{ ref('stg_rxclass__rxclass') }}
    where tty = 'PIN'

),

crosswalk_codes as (

    select
        *
    from {{ ref('stg_umls__crosswalk_codes') }}
)

select
    precise_ingredients.rxcui as precise_ingredient_rxcui,
    precise_ingredients.name as precise_ingredient_name,
    precise_ingredients.tty as precise_ingredient_tty,
    precise_ingredients.rela,
    precise_ingredients.class_id,
    precise_ingredients.class_name,
    precise_ingredients.class_type,
    precise_ingredients.rela_source,
    crosswalk_codes.*
from precise_ingredients
inner join crosswalk_codes
    on crosswalk_codes.from_code = precise_ingredients.class_id
