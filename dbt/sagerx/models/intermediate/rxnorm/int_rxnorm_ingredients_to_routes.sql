with 

clinical_product_components as (
	select * from {{ ref('stg_rxnorm__clinical_product_components') }}
),

ingredients as (
	select * from {{ ref('stg_rxnorm__ingredients') }}
),

dose_forms as (
	select * from {{ ref('stg_rxnorm__dose_forms') }}
),

dose_form_group_links as (
	select * from {{ ref('stg_rxnorm__dose_form_group_links') }}
),

dose_form_groups as (
	select * from {{ ref('stg_rxnorm__dose_form_groups') }}
),

dose_form_groups_to_routes as (
	select * from {{ ref('rxnorm_dose_form_groups_to_routes') }}
)

select distinct
    ing.rxcui as ingredient_rxcui
	, ing.name as ingredient_name
	, route.route_name
	, ing.active
	, ing.prescribable
from clinical_product_components prod
left join ingredients ing
	on ing.rxcui = prod.ingredient_rxcui
left join dose_forms df
	on df.rxcui = prod.dose_form_rxcui
left join dose_form_group_links dfgl
	on dfgl.dose_form_rxcui = df.rxcui
left join dose_form_groups dfg
	on dfg.rxcui = dfgl.dose_form_group_rxcui
left join dose_form_groups_to_routes route
	-- NOTE: not sure why cast is needed since the column type
	--       is defined in dbt_projet.yml
	on cast(route.dose_form_group_rxcui as varchar) = dfg.rxcui
where route.route_name is not null
