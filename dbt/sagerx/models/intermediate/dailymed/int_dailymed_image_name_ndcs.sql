-- int_dailymed_image_name_ndcs

with

package_label_section_images as (

    select * from {{ ref('stg_dailymed__package_label_section_images') }}

),

regex_ndcs as (

    select
        *,
        (regexp_matches(image, '(?:\d{4}|\d{5})-\d{3,6}(?:-\d{1,2})?|\d{11}|\d{10}', 'g'))[1] as regex_ndc
    from package_label_section_images

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

    select
        regex_ndcs.*,
        spl_ndc.ndc,
        spl_ndc.ndc11
    from regex_ndcs
    inner join valid_spl_ndcs spl_ndc
        on spl_ndc.set_id = regex_ndcs.set_id
        and {{ ndc_to_11('spl_ndc.ndc') }} = {{ ndc_to_11('regex_ndcs.regex_ndc') }}

)

select * from validated_ndcs
