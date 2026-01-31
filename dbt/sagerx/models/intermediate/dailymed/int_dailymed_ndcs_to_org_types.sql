-- int_dailymed_ndcs_to_org_types.sql

select distinct
    dm_ndcs.*,
    dm_orgs.dun,
    dm_orgs.org_name,
    dm_orgs.org_type
from {{ ref('stg_dailymed__ndcs') }} dm_ndcs
left join {{ ref('stg_dailymed__organizations') }} dm_orgs
    on dm_orgs.set_id = dm_ndcs.set_id
    and dm_orgs.version_number = dm_ndcs.version_number
