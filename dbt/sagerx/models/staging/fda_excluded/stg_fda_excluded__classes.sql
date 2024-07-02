-- stg_fda_excluded__classes.sql
with

product as (
    select * from {{ source('fda_excluded', 'fda_excluded_product') }}
)

, pharm_classes_array as (
	select 
		product.productid
		, token
		, row_number() over (partition by product.productid order by token desc) as class_line
	from product, unnest(string_to_array(product.pharm_classes, ',')) as token
)

select
	classes.productid
	, classes.class_line
	, trim(left(classes.token, position('[' in classes.token) -1 )) as class_name
	, substring(classes.token, '\[(.+)\]') as class_type
from pharm_classes_array classes
order by
	productid
	, class_line
