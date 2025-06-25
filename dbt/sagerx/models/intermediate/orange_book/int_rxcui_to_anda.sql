select
	rxn.product_rxcui,
	rxn.product_name,
	ndc.applicationnumber,
	ndc11 as ndc
from {{ ref('stg_fda_ndc__ndcs') }} ndc
left join {{ ref('int_rxnorm_ndcs_to_products') }} rxn
	on rxn.ndc = ndc.ndc11
where applicationnumber like('ANDA%')
	or applicationnumber like('NDA%')
order by applicationnumber, product_rxcui
