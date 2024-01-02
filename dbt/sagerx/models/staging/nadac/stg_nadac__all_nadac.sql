-- stg_nadac__all_nadac.sql

with

nadac as (

    select * from {{ source('nadac','nadac') }}

)

select distinct 
	ndc
	, ndc_description
	, nadac_per_unit::numeric
	, pricing_unit
	, effective_date::date
from nadac
