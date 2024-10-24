--int_dailymed_validated_package_label_ndcs

with

valid_spl_ndcs as (

    select * from{{ ref('stg_dailymed__ndcs') }}

),

package_label_ndc_matches as (

    select * from {{ ref('stg_dailymed__package_label_section_ndcs') }}

),

validated_package_ndcs as (

    select
        *
    from package_label_ndc_matches pkg_ndc
    where exists (

        select
            ndc
        from valid_spl_ndcs
        where ndc = pkg_ndc.ndc
        
    )

)

select * from validated_package_ndcs
