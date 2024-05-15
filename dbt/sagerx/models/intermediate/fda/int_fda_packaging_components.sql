{{ config(materialized='table') }}

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

select
	z.ndc11
    , z.packagedescription
   	, z.ordinality as component_line
	, trim(z.token) as component_text
	, trim(substring(z.token from '(.*) in ')) as inner_text
	, trim(substring(z.token from ' in (.*?)(?:\(|$)')) as outer_text
	, trim(substring(z.token from '\((.+)\)')) as outer_ndc
	, ndc_to_11(trim(substring(z.token from '\((.+)\)'))) as outer_ndc11
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
