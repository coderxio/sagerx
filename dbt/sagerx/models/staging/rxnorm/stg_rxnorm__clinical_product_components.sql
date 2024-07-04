-- stg_rxnorm__clinical_product_components.sql

with product as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, rxnrel as (
	select * from {{ source('rxnorm', 'rxnorm_rxnrel') }} 
)

, ingredient as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, product_component as (
	select * from {{ source('rxnorm', 'rxnorm_rxnconso') }} 
)

, cte as (
	select
	sq.*,
	row_number() over(partition by product_component_rxcui order by ingredient_tty desc) as rn
	from (
		select
			product_component.rxcui as product_component_rxcui
			, product_component.str as product_component_name
			, product_component.tty as product_component_tty
			, ingredient.rxcui as ingredient_rxcui
			, ingredient.str as ingredient_name
			, ingredient.tty as ingredient_tty
		from product_component
		inner join rxnrel 
			on rxnrel.rxcui2 = product_component.rxcui 
			and rxnrel.rela = 'has_ingredients'
		inner join ingredient
			on rxnrel.rxcui1 = ingredient.rxcui
			and ingredient.tty = 'MIN'
			and ingredient.sab = 'RXNORM'
		where product_component.tty = 'SCD'
			and product_component.sab = 'RXNORM'

		union all

		select
			product_component.rxcui as product_component_rxcui
			, product_component.str as product_component_name
			, product_component.tty as product_component_tty
			, ingredient.rxcui as ingredient_rxcui
			, ingredient.str as ingredient_name
			, ingredient.tty as ingredient_tty
		from  product_component
		inner join rxnrel as scdc_rxnrel
			on scdc_rxnrel.rxcui2 = product_component.rxcui 
			and scdc_rxnrel.rela = 'consists_of'
		inner join product as scdc 
			on scdc_rxnrel.rxcui1 = scdc.rxcui
		inner join rxnrel as ingredient_rxnrel 
			on ingredient_rxnrel.rxcui2 = scdc.rxcui 
			and ingredient_rxnrel.rela = 'has_ingredient'
		inner join ingredient
			on ingredient_rxnrel.rxcui1 = ingredient.rxcui
			and ingredient.tty = 'IN'
			and ingredient.sab = 'RXNORM'
		where product_component.tty = 'SCD'
			and product_component.sab = 'RXNORM'
	) sq
)

select distinct
	case when product_component.rxcui is null then product.rxcui else product_component.rxcui end rxcui
	, case when product_component.str is null then product.str else product_component.str end name 
	, case when product_component.tty is null then product.tty else product_component.tty end tty
	, case when 
		case when product_component.rxcui is null then product.suppress else product_component.suppress end = 'N' then true else false end as active
	, case when 
		case when product_component.rxcui is null then product.cvf else product_component.cvf end = '4096' then true else false end as prescribable
	, cte.ingredient_rxcui as ingredient_rxcui
	, dose_form_rxnrel.rxcui1 as dose_form_rxcui
from  product
left join rxnrel 
	on rxnrel.rxcui2 = product.rxcui 
	and rxnrel.rela = 'contains'
left join product_component
	on rxnrel.rxcui1 = product_component.rxcui
    and product_component.tty = 'SCD'
    and product_component.sab = 'RXNORM'
left join cte 
	on cte.product_component_rxcui = case when product_component.rxcui is null then product.rxcui else product_component.rxcui end
	and cte.rn < 2
left join rxnrel as dose_form_rxnrel
	on dose_form_rxnrel.rxcui2 = case when product_component.rxcui is null then product.rxcui else product_component.rxcui end
	and dose_form_rxnrel.rela = 'has_dose_form'
	and dose_form_rxnrel.sab = 'RXNORM'
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM'
