with clinical_products_to_ingredient_strengths as (
	select *
	from intermediate.int_rxnorm_clinical_products_to_ingredient_strengths
	order by ingredient_component_name asc
    -- NOTE: would it be more stable to sort each component by rxcui asc?
)

, cte as (
	select
		clinical_product_rxcui
		, clinical_product_name
		, string_agg(ingredient_component_name, ' / ') as ingredient_name
		, string_agg(coalesce(precise_ingredient_name, ingredient_component_name), ' / ') as precise_ingredient_name
		, string_agg(distinct dose_form_name, ' / ') as dose_form_name
		, string_agg(strength_text, ' / ') as strength_name
	from clinical_products_to_ingredient_strengths
	group by
		clinical_product_rxcui
		, clinical_product_name
)

select distinct
    atc_1_name
    , atc_2_name
    , atc_3_name
    , atc_4_name
	, clinical_product_rxcui
	, clinical_product_name
	, cte.ingredient_name as ingredient
	, precise_ingredient_name as precise_ingredient
    , concat(
            precise_ingredient_name
            , ' '
            , dose_form_name
        ) as dose_form
    , concat(
            precise_ingredient_name
            , ' '
            , dose_form_name
            , ' '
            , strength_name
        ) as strength
from cte
left join intermediate.int_atc_codes_to_product_rxcuis atc
    on atc.rxcui::varchar = cte.clinical_product_rxcui
order by clinical_product_rxcui
