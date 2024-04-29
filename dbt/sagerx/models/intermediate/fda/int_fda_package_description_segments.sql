with 

all_fda as (
	select ndc11, packagedescription
	from sagerx_dev.stg_fda_ndc__ndcs

	union all

	select ndc11, packagedescription
	from sagerx_dev.stg_fda_excluded__ndcs

	union all

	select ndc11, packagedescription
	from sagerx_dev.stg_fda_unfinished__ndcs
)

select
	z.ndc11
    , z.packagedescription
   	, z.ordinality as package_line
	, trim(z.token) as package_text
from (
    select distinct 
        all_fda.ndc11
	    , all_fda.packagedescription
	    , s.token
        , s.ordinality
	from all_fda, unnest(string_to_array(all_fda.packagedescription, '/')) with ordinality as s(token, ordinality)
) z
order by ndc11, package_line