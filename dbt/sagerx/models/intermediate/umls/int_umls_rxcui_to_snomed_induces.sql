select
	umls.rxcui,
	umls.name,
	umls.snomedct_us_code as concept_code,
	umls.snomedct_us_description as concept_name
from sagerx_lake.umls_condition_crosswalk umls
where rela = 'induces'
and snomedct_us_code is not null