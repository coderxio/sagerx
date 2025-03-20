-- atc_codes_to_rxnorm_products

with rxclass_atcprod as (

	select * from {{ ref('stg_rxclass__atcprod') }}

)

, atc_codes as (

    select * from {{ ref('stg_rxnorm__atc_codes') }}

)

, rxnorm_product_rxcuis as (

    select * from {{ ref('stg_rxnorm__product_rxcuis') }}

)

select distinct
	rxclass_atcprod.rxcui
	, rxnorm_product_rxcuis.str as rxnorm_description
	, atc_codes.atc_1_code
	, atc_codes.atc_2_code
	, atc_codes.atc_3_code
	, atc_codes.atc_4_code
	, atc_codes.atc_1_name
	, atc_codes.atc_2_name
	, atc_codes.atc_3_name
	, atc_codes.atc_4_name
from rxclass_atcprod
left join atc_codes
	on atc_codes.atc_4_code = rxclass_atcprod.class_id
left join rxnorm_product_rxcuis
	on rxnorm_product_rxcuis.rxcui = rxclass_atcprod.rxcui
order by rxcui
