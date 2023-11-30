/* intermediate.int_dailymed_organization_metrics */

with dailymed_main as (
    select * from {{ ref('stg_dailymed__main') }}
),

dailymed_organizations as (
    select * from {{ ref('stg_dailymed__organizations') }}
),

dailymed_organization_texts as (
    select * from {{ ref('stg_dailymed__organization_texts') }}
)

select o.set_id
	, ma.market_status
	, sum(case when org_type = 'Functioner' then 1 else 0 end) as functioner_count
	, sum(case when org_type = 'Labeler' then 1 else 0 end) as labeler_count
	, sum(case when org_type = 'Repacker' then 1 else 0 end) as repacker_count
	, case when sum(case when ot.set_id is not null then 1 else 0 end) > 0 then 'Yes' else '' end as organization_text
	, case when sum(case when org_type = 'Labeler' then 1 else 0 end) = 1 
				and sum(case when org_type = 'Functioner' then 1 else 0 end) = 0
			then 'yes' else '' end as labeler_only
	, count(*)
from dailymed_main ma
	inner join dailymed_organizations o
        on o.set_id = ma.set_id
	left join dailymed_organization_texts ot
        on o.set_id = ot.set_id
group by o.set_id, ma.market_status
