-- int_nadac_all_pricing.sql

with

current_release as (
/*
    current release is where the as_of_date is the same
    as the max(as_of_date)

    we use this CTE as a flag later on in this data model
    for people who want to only see the current release pricing
*/

    select distinct
        ndc,
        nadac_per_unit,
        effective_date
    from {{ ref('stg_nadac__nadac') }}
    where as_of_date = (select max(as_of_date) from {{ ref('stg_nadac__nadac') }})

),

most_recent_release as (
/*
    the most recent release may not be the current release
    in cases where an NDC falls off of the list in the current
    NADAC release (see current releases above)

    in this case, the most recent release will be the
    most recent as_of_date for a given NDC

    we use this CTE to filter out prices where the as_of_date
    increases, but the effective_date decreases

    we also use it to de-duplicate variations in ndc_description
*/

    select
        ndc,
        ndc_description,
        effective_date
    from (
        select
            *,
            row_number() over (
                partition by ndc
                order by as_of_date desc
            ) as rn
        from {{ ref('stg_nadac__nadac') }}
    ) ranked
    where rn = 1

),

distinct_nadac_prices as (
/*
    mainly we want to filter out as_of_date which
    is just an indicator of which release the data
    came from

    also - ndc_description can change over time so
    we exclude it here and pull it instead from the
    most_recent_release cte
*/

    select distinct
		nadac.ndc,
        most_recent_release.ndc_description,
		nadac.nadac_per_unit,
		nadac.pricing_unit,
		nadac.effective_date,
        max(as_of_date) over (
            partition by
                nadac.ndc,
                nadac.nadac_per_unit,
                nadac.effective_date
        ) as max_as_of_for_price_and_effective_date
    from {{ ref('stg_nadac__nadac') }} nadac
    left join most_recent_release
        on most_recent_release.ndc = nadac.ndc
    -- this filters out prices where the as_of_date increases, but
    -- the effective_date decreasees
    where nadac.effective_date <= most_recent_release.effective_date

),

ranked_price_start_dates as (
	
	select 
		ndc,
        ndc_description,
		nadac_per_unit,
		pricing_unit,
		effective_date as start_date,
        -- the most recent effective_date will normally have null for an end_date
        -- however - the end date is really the last time that price was in a NADAC release
        -- this coalesce handles that and adds 6 days to the most recent as_of_date to
        -- account for the price being valid during the week of the most recent release
		coalesce(
            lag(effective_date, 1) over (partition by ndc order by effective_date desc) - 1,
            max_as_of_for_price_and_effective_date + 6
        ) as end_date,
        lag(nadac_per_unit, 1) over (partition by ndc order by effective_date asc) as previous_nadac_per_unit,
        row_number() over (partition by ndc order by max_as_of_for_price_and_effective_date desc) as price_line,
        count(*) over (partition by ndc) as max_price_line
	from distinct_nadac_prices

),

nadac_historical_pricing as (

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
        case
            when (nadac_per_unit - previous_nadac_per_unit) > 0
                then 1
            when (nadac_per_unit - previous_nadac_per_unit) = 0
                then 0
            when (nadac_per_unit - previous_nadac_per_unit) is null
                then null
            else -1
            end as change_type
    from ranked_price_start_dates
    order by
        ndc,
        start_date

),

final as (

    select
        nadac_historical_pricing.*,
        case when current_release.ndc is not null
            then true
            else false
        end as is_from_current_release,
        end_date >= current_date - interval '30 days' as is_within_30_days,
        end_date >= current_date - interval '60 days' as is_within_60_days,
        end_date >= current_date - interval '90 days' as is_within_90_days,
        end_date >= current_date - interval '180 days' as is_within_180_days,
        end_date >= current_date - interval '365 days' as is_within_365_days
    from nadac_historical_pricing
    left join current_release
        on current_release.ndc = nadac_historical_pricing.ndc
        and current_release.nadac_per_unit = nadac_historical_pricing.nadac_per_unit
        and current_release.effective_date = nadac_historical_pricing.start_date
    order by start_date desc

)

select * from final
