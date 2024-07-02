-- stg_fda_unfinished__substances.sql

with

product as (    
    select * from {{ source('fda_unfinished', 'fda_unfinished_product') }}
)

, substancename_array as (
	select
		productid
		, substance
		, row_number() over(partition by productid) as substance_line
	from product, unnest(string_to_array(substancename, '; ')) as substance
)

, strength_array as (
	select
		productid
		, strength
		, row_number() over(partition by productid) as strength_line
	from product, unnest(string_to_array(active_numerator_strength, '; ')) as strength
)

, unit_array as (
	select
		productid
		, unit
		, row_number() over(partition by productid) as unit_line
	from product, unnest(string_to_array(active_ingred_unit, '; ')) as unit
)

select
	substance.productid
	, substance.substance_line
	, substance.substance as substancename
	, strength.strength as active_numerator_strength
	, unit.unit as active_ingred_unit
from substancename_array substance
inner join strength_array strength
	on strength.productid = substance.productid
	and strength.strength_line = substance.substance_line
inner join unit_array unit
	on unit.productid = substance.productid
	and unit.unit_line = substance.substance_line
order by
	productid
	, substance_line