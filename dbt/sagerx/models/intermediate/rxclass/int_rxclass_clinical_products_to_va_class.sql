select distinct
	rs.atv as clinical_product_ndc,
	rcl.rxcui as clinical_product_rxcui,
	rcl.name as clinical_product_name,
	rcl.class_id as clinical_product_code,
	rcl.class_name as clinical_product_class_name
from sagerx_lake.rxnorm_rxnsat rs
join sagerx_lake.rxnorm_rxnconso rc on rs.rxaui = rc.rxaui
join sagerx_lake.rxclass rcl on rcl.rxcui = rs.rxcui
where
	rs.atn = 'NDC'
	and rc.sab = 'RXNORM'
	and rcl.rela_source = 'VA'