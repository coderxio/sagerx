with

images as (

    select * from {{ ref('stg_dailymed__package_label_section_images') }}

),

regex_ndcs as (

    select
        *,
        (regexp_matches(image, '\d+-\d+-\d+', 'g'))[1] as ndc
    from images

),

valid_spl_ndcs as (

    select * from {{ ref('stg_dailymed__ndcs') }}

),

validated_ndcs as (

    select
        regex_ndcs.*
    from regex_ndcs
    inner join valid_spl_ndcs spl_ndc
        on spl_ndc.ndc = regex_ndcs.ndc

)

select * from validated_ndcs
