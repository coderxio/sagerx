{{ config(materialized='view') }}

with 

packaging_components as (
	select * from {{ ref('int_fda_packaging_components') }}
	where component_text like ('%*%')
)

select
	z.ndc11
    , z.packagedescription
	, z.component_line
	, z.component_text
   	, z.ordinality as part_line
	, trim(z.token) as part_text
from (
    select distinct 
        components.*
	    , s.token
        , s.ordinality
	from
		packaging_components components
		, unnest(
			string_to_array(
				component_text
				, '*')
			) with ordinality as s(token, ordinality)
) z
order by ndc11, component_line, part_line
