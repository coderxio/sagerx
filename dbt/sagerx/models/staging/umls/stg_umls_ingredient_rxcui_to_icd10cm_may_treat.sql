select
	umls.rxcui,
	umls.name,
	umls.icd10cm_code as concept_code,
	umls.icd10cm_description as concept_name
from sagerx_lake.umls_condition_crosswalk umls
where rela = 'may_treat'
and icd10cm_code is not null