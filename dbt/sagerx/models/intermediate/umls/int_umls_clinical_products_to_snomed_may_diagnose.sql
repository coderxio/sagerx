select distinct
	ndc.ndc as clinical_product_ndc,
	ndc.clinical_product_rxcui,
	ndc.clinical_product_name,
	umls.concept_code,
	umls.concept_name
from {{ ref('stg_umls_ingredient_rxcui_to_snomed_may_diagnose') }} umls
join {{ ref('int_rxnorm_clinical_products_to_ndcs') }} ndc
	on umls.rxcui = ndc.ingredient_rxcui
where ndc is not null