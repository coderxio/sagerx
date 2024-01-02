with atc_codes_to_products as (
	select * from datasource.rxclass_atc_to_product
),

atc_codes as (
    select * from {{ ref('stg_rxnorm__atc_codes') }}
)

select
	atc_codes.*
	, atc_codes_to_products.rxcui
from atc_codes_to_products
left join atc_codes
	on atc_codes.atc_4_code = atc_codes_to_products.class_id
order by
	atc_1_name
	, atc_2_name
	, atc_3_name
	, atc_4_name
	, atc_5_name
