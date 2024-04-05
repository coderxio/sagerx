-- stg_fda_ndc__ndcs.sql

with

product as (
    
    select * from {{ source('fda_ndc', 'fda_ndc_product') }}

),

package as (

    select * from {{ source('fda_ndc', 'fda_ndc_package') }}

)

select
	{{ndc_to_11 ('ndcpackagecode') }} as ndc11
	, package.productid
	, package.productndc
	, producttypename
	, proprietaryname
	, proprietarynamesuffix
	, nonproprietaryname
	, dosageformname
	, routename
	, product.startmarketingdate as product_startmarketingdate
	, product.endmarketingdate as product_endmarketingdate
	, marketingcategoryname
	, applicationnumber
	, labelername
	, substancename
	, active_numerator_strength
	, active_ingred_unit
	, pharm_classes
	, deaschedule
	, product.ndc_exclude_flag as product_ndc_exclude_flag
	, listing_record_certified_through
	, ndcpackagecode
	, packagedescription
	, package.startmarketingdate as package_startmarketingdate
	, package.endmarketingdate as package_endmarketingdate
	, package.ndc_exclude_flag as package_ndc_exclude_flag
	, sample_package
from package
left join product
	on package.productid = product.productid
