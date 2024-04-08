-- stg_fda_enforcement__json_ndcs.sql

WITH base AS (
	select
		fdae.recall_number
		, ndc.id_value #>> '{}' as ndc
		, app_num.id_value #>> '{}' as app_num
	from sagerx_lake.fda_enforcement fdae
		, json_array_elements(openfda->'package_ndc') with ordinality ndc(id_value, line)
		, json_array_elements(openfda->'application_number') with ordinality app_num(id_value, line)
) 

select
	fdae.recall_number
	, {{ndc_to_11 ('ndc')}} as ndc11
	, left({{ ndc_to_11 ('ndc')}},9) as ndc9
	, app_num
from sagerx_lake.fda_enforcement fdae
	, json_array_elements(openfda->'package_ndc') with ordinality ndc(id_value, line)
	, json_array_elements(openfda->'application_number') with ordinality app_num(id_value, line)
where {{ndc_to_11('ndc')}} is not null