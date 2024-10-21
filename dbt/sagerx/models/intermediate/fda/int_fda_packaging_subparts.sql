{{ config(materialized='view') }}

with 

packaging_parts as (
	select * from {{ ref('int_fda_packaging_parts') }}
	where part_text like ('%/%')
)

select
	z.ndc11
    , z.packagedescription
	, z.component_line
	, z.component_text
    , z.part_line
    , z.part_text
   	, z.ordinality as subpart_line
	, trim(z.token) as subpart_text
from (
    select distinct 
        parts.*
	    , s.token
        , s.ordinality
	from
		packaging_parts parts
		, unnest(
			string_to_array(
				part_text
				, '/')
			) with ordinality as s(token, ordinality)
) z
order by ndc11, component_line, part_line, subpart_line
