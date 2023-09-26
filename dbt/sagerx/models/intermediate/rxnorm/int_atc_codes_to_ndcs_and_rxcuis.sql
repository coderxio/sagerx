with atc_codes as (
    select * from {{ ref('stg_rxnorm__atc_codes') }}
)

, ingredients as (
    select * from {{ ref('stg_rxnorm__ingredients') }}
)

, clinical_products_to_ingredients as (
    select * from {{ ref('int_rxnorm_clinical_products_to_ingredients') }}
)

, clinical_products_to_ingredient_components as (
    select * from {{ ref('int_rxnorm_clinical_products_to_ingredient_components') }}
)

, ndcs_to_products as (
    select * from {{ ref('int_rxnorm_ndcs_to_products') }}
)

, atc_min_tty as (
	select distinct
		atc_1_name
		, atc_2_name
		, atc_3_name
		, atc_4_name
		, atc_5_name
		, ndc.ndc
		, prod.clinical_product_rxcui
		, prod.clinical_product_name
		, prod.clinical_product_tty
		, ndc.product_rxcui
		, ndc.product_name
		, ndc.product_tty
		, prod.active
		, prod.prescribable
	from atc_codes atc
	inner join ingredients ing
		on ing.rxcui = atc.ingredient_rxcui
		and ing.tty = 'MIN'
	/* NOTE: need to optimize this part - GPCKs with multiple SCDs that have
	   the same ingredient (i.e. oral contraceptives -> estradiol / norgestimate)
	   result in duplicate rows downstream and need for select distinct */
	inner join clinical_products_to_ingredients prod
		on prod.ingredient_rxcui = ing.rxcui
	left join ndcs_to_products ndc
		on ndc.clinical_product_rxcui = prod.clinical_product_rxcui
)


, atc_in_tty as (
	select distinct
		atc_1_name
		, atc_2_name
		, atc_3_name
		, atc_4_name
		, atc_5_name
		, ndc.ndc
		, prod.clinical_product_rxcui
		, prod.clinical_product_name
		, prod.clinical_product_tty
		, ndc.product_rxcui
		, ndc.product_name
		, ndc.product_tty
		, prod.active
		, prod.prescribable
	from atc_codes atc
	inner join ingredients ing
		on ing.rxcui = atc.ingredient_rxcui
		and ing.tty = 'IN'
	/* NOTE: need to optimize this part - GPCKs with multiple SCDs that have
	   the same ingredient (i.e. titration packs with multiple strengths)
	   result in duplicate rows downstream and need for select distinct */
	inner join clinical_products_to_ingredient_components prod
		on prod.ingredient_component_rxcui = ing.rxcui
	left join ndcs_to_products ndc
		on ndc.clinical_product_rxcui = prod.clinical_product_rxcui
)

select * from atc_min_tty
union
select * from atc_in_tty
order by
	atc_1_name
	, atc_2_name
	, atc_3_name
	, atc_4_name
	, atc_5_name
