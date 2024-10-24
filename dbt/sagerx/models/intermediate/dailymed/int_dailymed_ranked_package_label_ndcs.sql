with

validated_package_ndcs as (

    select * from {{ ref('int_dailymed_validated_package_label_ndcs') }}

),

ranked_package_ndcs as (

    select 
        *,
        row_number() over (
            partition by package_label_section_id
            order by id
        ) as rn
    from validated_package_ndcs

)

select * from ranked_package_ndcs
