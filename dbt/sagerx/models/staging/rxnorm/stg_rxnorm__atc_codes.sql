with atc as (
	select distinct a.rxcui
		,a.code
		,b.atn
		,b.atv as atc_class_level
		,a.str as description
		,a.sab
		,a.tty
	from (
		select *
		from sagerx_lake.rxnorm_rxnconso
		where sab = 'ATC'
		and tty not like 'RXN%'
		order by code
	) a
	left join sagerx_lake.rxnorm_rxnsat b
		on a.code= b.code
	where atn = 'ATC_LEVEL'
	order by code
)

, atc_5 as (
	select
		*
	from atc
	where atc_class_level = '5'
)

, atc_4 as (
	select
		*
	from atc
	where atc_class_level = '4'
)

, atc_3 as (
	select
		*
	from atc
	where atc_class_level = '3'
)

, atc_2 as (
	select
		*
	from atc
	where atc_class_level = '2'
)

, atc_1 as (
	select
		*
	from atc
	where atc_class_level = '1'
)

, sagerx_atc as (

select
	atc_1.code as atc_1_code
	, atc_1.description as atc_1_name
	, atc_2.code as atc_2_code
	, atc_2.description as atc_2_name
	, atc_3.code as atc_3_code
	, atc_3.description as atc_3_name
	, atc_4.code as atc_4_code
	, atc_4.description as atc_4_name
	, atc_5.code as atc_5_code
	, atc_5.description as atc_5_name
	, atc_5.rxcui as ingredient_rxcui
	, atc_5.description as ingredient_name
	, atc_5.tty as ingredient_tty

from atc_5
left join atc_4
	on left(atc_5.code, 5) = atc_4.code
left join atc_3
	on left(atc_4.code, 4) = atc_3.code
left join atc_2
	on left(atc_3.code, 3) = atc_2.code
left join atc_1
	on left(atc_2.code, 1) = atc_1.code
)
	
select * 
from sagerx_atc
