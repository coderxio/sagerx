-- stg_fda_ndc__ndc_associations

with package as (

    select * 
    from {{ source('fda_ndc', 'fda_ndc_package') }}

),

extracted_ndc as (

    select 
        package.ndcpackagecode,
        regexp_matches(package.packagedescription, '\d+-\d+-\d+', 'g') as ndc_match,
		packagedescription
    from package

),

ndc_array as (

    select 
        ndc.ndcpackagecode,
        unnest(ndc.ndc_match) as token,
		packagedescription
    from extracted_ndc ndc

),

ranked_array as (
	
	select
		ndcpackagecode,
		token,
		row_number() over() as rn,
		packagedescription
	from ndc_array

),

final_array as (

    select
        ndcpackagecode,
        token,
        row_number() over (partition by ndcpackagecode order by rn) as ndc_line,
		packagedescription
    from ranked_array

),

ndc_associations as (

    select
        ndcpackagecode as outer_ndc,
        {{ ndc_to_11('ndcpackagecode') }} as outer_ndc11,
        ndc_line,
        token as ndc,
        {{ ndc_to_11('token') }} as ndc11,
        packagedescription
    from final_array
    order by
        ndcpackagecode,
        ndc_line

)

select * from ndc_associations
