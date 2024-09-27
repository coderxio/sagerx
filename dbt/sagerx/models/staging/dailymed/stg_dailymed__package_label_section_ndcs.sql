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
		-- NOTE: accounting for potential whitespace and potential non-hyphen characters
		-- in NDCs and then stripping the whitespace
		-- TODO: should also replace the non-hyphen characters with hyphens
		regexp_replace((regexp_matches(p.text, '\d+\s*(?:-|–)\s*\d+\s*(?:-|–)\s*\d+', 'g'))[1], '\s', '', 'g') as ndc
	from package_label_sections p

),

id_ndcs as (

	select
		row_number() over() as id,
		*
	from ndcs

)

select * from id_ndcs
