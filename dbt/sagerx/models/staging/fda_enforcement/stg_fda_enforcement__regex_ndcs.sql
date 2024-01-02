-- stg_fda_enforcement__regex_ndcs.sql

with

z as (
	select
		recall_number
		, ndc_to_11((regexp_matches(product_description, '(\m\d{1,5}-\d{1,4}-\d{1,2}\M|\m\d{11}\M)', 'g'))[1]) as ndc11
		, left(ndc_to_11((regexp_matches(product_description, '(\m\d{1,5}-\d{1,4}-\d{1,2}\M|\m\d{11}\M)', 'g'))[1]), 9) as ndc9
	from datasource.fda_enforcement
)

select
	*
from z
where ndc11 is not null
