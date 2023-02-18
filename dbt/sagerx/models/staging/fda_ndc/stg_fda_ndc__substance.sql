-- stg_fda_ndc__substance.sql

select distinct
	prod.productid
	, row_number() over (partition by prod.productid) as substance_line
	, arr.*
from datasource.fda_ndc_product prod
	, unnest(string_to_array(prod.substancename, '; ')
		,string_to_array(prod.active_numerator_strength, '; ')
		,string_to_array(prod.active_ingred_unit, '; ')
	) arr(substancename,active_numerator_strength,active_ingred_unit)
