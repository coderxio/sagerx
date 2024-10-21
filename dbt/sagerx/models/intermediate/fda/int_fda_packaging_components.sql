{{ config(materialized='view') }}

with 

all_fda as (
	select ndc11, packagedescription
	from {{ ref('stg_fda_ndc__ndcs') }}

	union all

	select ndc11, packagedescription
	from {{ ref('stg_fda_excluded__ndcs') }}

	union all

	select ndc11, packagedescription
	from {{ ref('stg_fda_unfinished__ndcs') }}
)

, split_components as (
	select
		z.ndc11
		, z.packagedescription
		, z.ordinality as component_line
		, trim(z.token) as component_text
	from (
		select distinct 
			all_fda.ndc11
			, all_fda.packagedescription
			, s.token
			, s.ordinality
		from
			all_fda
			, unnest(
				string_to_array(
					regexp_replace(
						all_fda.packagedescription
						, '(?<!\*.*)\/'
						, ' | '
						, 'g')
					, '|')
				) with ordinality as s(token, ordinality)
	) z
	order by ndc11, component_line
)

, inner_outer_text as (
	select
		c.*
		, trim(substring(component_text from '(.*) in ')) as inner_text
		, trim(substring(component_text from ' in (.*?)(?:\(|$)')) as outer_text
		, trim(substring(component_text from '\((.+)\)')) as outer_ndc
	from split_components c
)

select
	*
	, {{ ndc_to_11('outer_ndc') }} as outer_ndc11
	, (regexp_match(inner_text, '^(\w+)\s(.*)'))[1] as inner_value
	, (regexp_match(inner_text, '(\w+)\s(.*)'))[2] as inner_unit
	, (regexp_match(outer_text, '^(\w+)\s(.*)'))[1] as outer_value
	, (regexp_match(outer_text, '(\w+)\s(.*)'))[2] as outer_unit
from inner_outer_text