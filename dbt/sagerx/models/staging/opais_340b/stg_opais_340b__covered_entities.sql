with

final as (
    select * from {{ source('opais_340b', 'opais_340b_covered_entities') }}
)

select * from final
