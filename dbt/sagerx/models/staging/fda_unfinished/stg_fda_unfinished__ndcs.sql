-- stg_fda_unfinished__ndcs.sql

with

product as (
    
    select * from {{ source('fda_unfinished', 'fda_unfinished_product') }}

),

package as (

    select * from {{ source('fda_unfinished', 'fda_unfinished_package') }}

)

select
	{{ndc_to_11 ('package.ndcpackagecode')}} as ndc11
	, package.productid
	, package.productndc
	, producttypename
	, nonproprietaryname
	, dosageformname
	, product.startmarketingdate as product_startmarketingdate
	, product.endmarketingdate as product_endmarketingdate
	, marketingcategoryname
	, labelername
	, substancename
	, active_numerator_strength
	, active_ingred_unit
	, deaschedule
	, listing_record_certified_through
	, ndcpackagecode
	, packagedescription
	, package.startmarketingdate as package_startmarketingdate
	, package.endmarketingdate as package_endmarketingdate
from package
left join product
	on package.productid = product.productid
