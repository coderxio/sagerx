-- int_nadac_all_pricing.sql

with

distinct_nadac_prices as (

    select distinct
		ndc,
		ndc_description,
		nadac_per_unit,
		pricing_unit,
		effective_date
    from {{ ref('stg_nadac__nadac') }}

),

ranked_price_start_dates as (
	
	select 
		ndc,
		ndc_description,
		nadac_per_unit,
		pricing_unit,
		effective_date as start_date,
		lag(effective_date, 1) over (partition by ndc order by effective_date desc) as end_date,
        lag(nadac_per_unit, 1) over (partition by ndc order by effective_date asc) as previous_nadac_per_unit,
        row_number() over (partition by ndc order by effective_date desc) as price_line,
        count(*) over (partition by ndc) as max_price_line
	from distinct_nadac_prices

),

nadac_all_pricing as (

    select
        ndc,
        ndc_description,
        nadac_per_unit,
        pricing_unit,
        start_date,
        end_date,
        case 
            when price_line = max_price_line
                then true 
            else false
            end as is_first_price,
        case
            when price_line = 1
                then true
            else false
            end as is_last_price,
        nadac_per_unit - previous_nadac_per_unit as dollar_change,
        (nadac_per_unit - previous_nadac_per_unit) / previous_nadac_per_unit as percent_change,
        case when (nadac_per_unit - previous_nadac_per_unit) > 0 then 1
            when (nadac_per_unit - previous_nadac_per_unit) = 0 then 0
            when (nadac_per_unit - previous_nadac_per_unit) is null then null
            else -1 end as change_type
    from ranked_price_start_dates
    order by
        ndc,
        start_date

)

select * from nadac_all_pricing
