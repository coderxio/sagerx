-- int_dailymed_repackers.sql

select
	org_name,
    org_type,
    count(*) as cnt
from {{ ref('stg_dailymed__organizations') }}
where org_type = 'Repacker'
group by 1,2
order by count(*) desc
