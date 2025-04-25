-- products_to_pregnancy_category.sql

select distinct
	c.clinical_product_rxcui,
	c.clinical_product_name,
	o.pregnancy_category
from {{ source('openfda', 'openfda_pregnancy_categories') }} o
join {{ ref('int_rxnorm_clinical_products_to_ndcs') }} c
on c.ndc = o.ndc