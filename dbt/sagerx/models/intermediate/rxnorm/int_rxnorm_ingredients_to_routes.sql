select distinct
    ing.rxcui as ingredient_rxcui
	, ing.name as ingredient_name
	, route.route_name
	, ing.active
	, ing.prescribable
from staging.stg_rxnorm__clinical_product_components prod
left join staging.stg_rxnorm__ingredients ing
	on ing.rxcui = prod.ingredient_rxcui
left join staging.stg_rxnorm__dose_forms df
	on df.rxcui = prod.dose_form_rxcui
left join staging.stg_rxnorm__dose_form_group_links dfgl
	on dfgl.dose_form_rxcui = df.rxcui
left join staging.stg_rxnorm__dose_form_groups dfg
	on dfg.rxcui = dfgl.dose_form_group_rxcui
left join seeds.rxnorm_dose_form_group_to_route route
	on route.dose_form_group_rxcui = dfg.rxcui
where route.route_name is not null
