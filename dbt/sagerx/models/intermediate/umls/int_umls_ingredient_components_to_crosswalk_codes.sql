-- int_umls_ingredient_components_to_crosswalk_codes.sql

with

ingredient_components as (

    select
        *
    from {{ ref('stg_rxclass__rxclass') }}
    where tty = 'IN'

),

crosswalk_codes as (

    select
        *
    from {{ ref('stg_umls__crosswalk_codes') }}
)

select
    ingredient_components.rxcui as ingredient_component_rxcui,
    ingredient_components.name as ingredient_component_name,
    ingredient_components.tty as ingredient_component_tty,
    ingredient_components.rela,
    ingredient_components.class_id,
    ingredient_components.class_name,
    ingredient_components.class_type,
    ingredient_components.rela_source,
    crosswalk_codes.*
from ingredient_components
inner join crosswalk_codes
    on crosswalk_codes.from_code = ingredient_components.class_id
