-- stg_rxnorm__brands.sql

with cte as (
	select
		sq.*
		, row_number() over(partition by rxcui order by ingredient_tty desc) as rn
	from (

		select
			brand.rxcui as rxcui
			, brand.str as name
			, brand.tty as tty
			, ingredient.rxcui as ingredient_rxcui
			, ingredient.str as ingredient_name
			, ingredient.tty as ingredient_tty
		from sagerx_lake.rxnorm_rxnconso brand
		inner join sagerx_lake.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = brand.rxcui and rxnrel.rela = 'tradename_of'
		inner join sagerx_lake.rxnorm_rxnconso ingredient
			on rxnrel.rxcui1 = ingredient.rxcui
			and ingredient.tty = 'IN'
			and ingredient.sab = 'RXNORM'
		where brand.tty = 'BN'
			and brand.sab = 'RXNORM'

		union all

		select
			brand.rxcui as rxcui
			, brand.str as name
			, brand.tty as tty
			, ingredient.rxcui as ingredient_rxcui
			, ingredient.str as ingredient_name
			, ingredient.tty as ingredient_tty
		from sagerx_lake.rxnorm_rxnconso brand
		inner join sagerx_lake.rxnorm_rxnrel sbd_rxnrel on sbd_rxnrel.rxcui2 = brand.rxcui and sbd_rxnrel.rela = 'ingredient_of'
		inner join sagerx_lake.rxnorm_rxnrel scd_rxnrel on scd_rxnrel.rxcui2 = sbd_rxnrel.rxcui1 and scd_rxnrel.rela = 'tradename_of'
		inner join sagerx_lake.rxnorm_rxnrel ingredient_rxnrel on ingredient_rxnrel.rxcui2 = scd_rxnrel.rxcui1 and ingredient_rxnrel.rela = 'has_ingredients'
		left join sagerx_lake.rxnorm_rxnconso ingredient
			on ingredient_rxnrel.rxcui1 = ingredient.rxcui
			and ingredient.tty = 'MIN'
			and ingredient.sab = 'RXNORM'		
		where brand.tty = 'BN'
			and brand.sab = 'RXNORM'
	) sq
)

select distinct
	brand.rxcui as rxcui
	, brand.str as name
	, brand.tty as tty
	, case when brand.suppress = 'N' then true else false end as active
	, case when brand.cvf = '4096' then true else false end as prescribable
	, cte.ingredient_rxcui as ingredient_rxcui
from sagerx_lake.rxnorm_rxnconso product
inner join sagerx_lake.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'has_ingredient'
inner join sagerx_lake.rxnorm_rxnconso brand
	on rxnrel.rxcui1 = brand.rxcui
	and brand.tty = 'BN'
	and brand.sab = 'RXNORM'
Left join cte on cte.rxcui = brand.rxcui and cte.rn < 2
where product.tty = 'SBD'
	and product.sab = 'RXNORM'
