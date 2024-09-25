 /* stg_dailymed__package_label_section_ndcs */

with

package_label_sections as
(
	select * from {{ ref('stg_dailymed__package_label_sections') }}
),

ndcs as (

	select
		p.set_id,
		p.id as package_label_section_id,
		-- NOTE: removing whitespace with RegEx to account for line breaks and extra
		-- whitespace padding betweeen NDC segments
		(regexp_matches(regexp_replace(p.text, '\s', '', 'g'), '\d+-\d+-\d+', 'g'))[1] as ndc
	from package_label_sections p

),

id_ndcs as (

	select
		row_number() over() as id,
		*
	from ndcs

)

select * from id_ndcs
