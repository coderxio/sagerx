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
		-- TODO: account for NDCs with spaces instead of dashes
		-- example ndc 55292 140 01
		-- example set_id a0aad470-3f38-af97-e053-2995a90a383a
		regexp_replace(regexp_replace((regexp_matches(p.text, '(?:\d{4}|\d{5})\s*(?:-|–)\s*\d{3,6}\s*(?:-|–)\s*\d{1,2}|\d{11}|\d{10}', 'g'))[1], '\s', '', 'g'), '–', '-') as ndc
	from package_label_sections p

),

id_ndcs as (

	select
		row_number() over() as id,
		*
	from ndcs

)

select * from id_ndcs
