-- stg_nadac__enhanced_nadac.sql

with

all_nadac as (

    select * from {{ ref('stg_nadac__all_nadac') }}

),

nadac as (
	
	select ndc
		, ndc_description
		, row_number() over (partition by ndc order by effective_date desc) as price_line
		, effective_date as price_start_date
		, lag(effective_date, 1) over (partition by ndc order by effective_date desc) price_end_date
		, nadac_per_unit
		, pricing_unit
		from all_nadac

)

select
	ndc
	, ndc_description
	, price_line
	, nadac_per_unit
	, pricing_unit 
	, price_start_date
	, case when price_line = 1 then true else false end as most_recent_price
	, case when price_line = first_value(price_line) over (partition by ndc order by price_line desc) then true else false end as first_price
	, (nadac_per_unit - lag(nadac_per_unit) over (partition by ndc order by price_line desc)) as dollar_change
	, (nadac_per_unit - lag(nadac_per_unit) over (partition by ndc order by price_line desc)) /
		lag(nadac_per_unit) over (partition by ndc order by price_line desc) as percent_change
	, case when (nadac_per_unit - lag(nadac_per_unit) over (partition by ndc order by price_line desc)) > 0 then 1
		when (nadac_per_unit - lag(nadac_per_unit) over (partition by ndc order by price_line desc)) = 0 then 0
		when (nadac_per_unit - lag(nadac_per_unit) over (partition by ndc order by price_line desc)) is null then null
		else -1 end as change_type
from nadac
