 /* staging.stg_dailymed__package_label_section_ndcs */

with package_label_sections as
(
	select * from {{ ref('stg_dailymed__package_label_sections') }}
)

select
	p.id as package_label_section_id
	, (regexp_matches(p.text, '\d+-\d+-\d+', 'g'))[1] as ndc
from package_label_sections p
