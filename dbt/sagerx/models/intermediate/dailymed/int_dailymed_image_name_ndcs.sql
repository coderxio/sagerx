with

images as (

    select * from {{ ref('stg_dailymed__package_label_section_images') }}

),

regex_ndcs as (

    select
        *,
        (regexp_matches(image, '\d+-\d+(?:-\d+)?|d{11}|d{10}', 'g'))[1] as ndc
    from images

    /*
        \d{11}              | # 11 digit
        \d{10}              | # 10 digit
        \d{5}-\d{5}         | # 5-5
        \d{5}-\d{4}-\d{2}   | # 5-4-2
        \d{5}-\d{4}-\d{1}   | # 5-4-1
        \d{5}-\d{3}-\d{2}   | # 5-3-2
        \d{4}-\d{6}         | # 4-6
        \d{4}-\d{4}-\d{2}     # 4-4-2

    */

),

valid_spl_ndcs as (

    select * from {{ ref('stg_dailymed__ndcs') }}

),

validated_ndcs as (

    -- TODO: convert SPL NDCs to NDC11 and hyphenless and compare
    -- to regex_ndc - but select the SPL version of the NDC
    select
        regex_ndcs.*
    from regex_ndcs
    inner join valid_spl_ndcs spl_ndc
        on spl_ndc.ndc = regex_ndcs.ndc

)

select * from validated_ndcs
