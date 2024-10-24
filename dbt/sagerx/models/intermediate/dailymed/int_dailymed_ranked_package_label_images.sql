with

package_label_images as (

    select * from{{ ref('stg_dailymed__package_label_section_images') }}

),

ranked_package_images as (

    select 
        *,
        row_number() over (
            partition by package_label_section_id
            order by id
        ) as rn
    from package_label_images

)

select * from ranked_package_images
