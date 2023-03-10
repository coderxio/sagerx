-- stg_nadac__all_nadac.sql

with

nadac as (

    select * from {{ source('nadac','nadac') }}

)

select distinct 
	n.ndc
	, n.ndc_description
	, n.nadac_per_unit::numeric
	, n.pricing_unit
	, n.effective_date::date
from nadac n
