select
	umls.rxcui,
	umls.name,
	umls.icd9cm_code as concept_code,
	umls.icd9cm_description as concept_name
from sagerx_lake.umls_condition_crosswalk umls
where rela = 'may_diagnose'
and icd9cm_code is not null