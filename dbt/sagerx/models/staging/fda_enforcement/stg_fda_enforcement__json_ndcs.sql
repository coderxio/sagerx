-- stg_fda_enforcement__json_ndcs.sql

{{ config(materialized='table') }}

select
	fdae.recall_number
	, ndc_to_11(ndc.id_value #>> '{}') as ndc11
	, left(ndc_to_11(ndc.id_value #>> '{}'),9) as ndc9
	, app_num.id_value #>> '{}' as app_num
from datasource.fda_enforcement fdae
	, json_array_elements(openfda->'package_ndc') with ordinality ndc(id_value, line)
	, json_array_elements(openfda->'application_number') with ordinality app_num(id_value, line)
where ndc_to_11(ndc.id_value #>> '{}') is not null
