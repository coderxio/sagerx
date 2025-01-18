with

packaging_components as (

    select
        *
    from {{ ref('int_fda_packaging_components') }}

),

innermost_unit as (

    select ndc11, inner_unit
    from (
        select ndc11, inner_unit, 
            row_number() over (partition by ndc11 order by component_line desc) as row_num
        from packaging_components
    ) as ranked
    where row_num = 1

),

outermost_unit as (

    select ndc11, outer_unit
    from packaging_components
    where component_line = 1

)

select distinct
    packaging_components.ndc11,
    outermost_unit.outer_unit as outermost_unit,
    total_product,
    case
        when innermost_unit.inner_unit like('%KIT %')
            then 'KIT' 
        else innermost_unit.inner_unit 
    end as innermost_unit,
    packagedescription
from packaging_components
left join innermost_unit
    on innermost_unit.ndc11 = packaging_components.ndc11
left join outermost_unit
    on outermost_unit.ndc11 = packaging_components.ndc11
