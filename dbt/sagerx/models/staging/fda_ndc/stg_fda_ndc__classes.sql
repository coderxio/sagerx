-- stg_fda_ndc__classes.sql

with

product as (
    
    select * from {{ source('fda_ndc', 'fda_ndc_product') }}

)

select
	z.productid
	, row_number() over (partition by z.productid order by z.token desc) as class_line
	, trim(left(z.token, position('[' in z.token) -1 )) as class_name
    , substring(z.token, '\[(.+)\]') as class_type
from (select distinct product.productid
	, product.pharm_classes
	, s.token
	from product, unnest(string_to_array(product.pharm_classes, ',')) s(token)) z
