-- stg_nadac__nadac.sql

with

nadac as (

    select distinct 
		ndc_description,
		ndc,
		nadac_per_unit::numeric,
		effective_date::date,
		pricing_unit,
		pharmacy_type_indicator,
		otc,
		explanation_code,
		classification_for_rate_setting,
		corresponding_generic_drug_nadac_per_unit,
		corresponding_generic_drug_effective_date,
		as_of_date
	from {{ source('nadac','nadac') }}

)

select * from nadac
