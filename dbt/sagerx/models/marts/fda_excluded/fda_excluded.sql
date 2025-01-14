SELECT
	{{ndc_to_11('pack.ndcpackagecode')}} AS ndc11
	, pack.productid
	, pack.productndc
	, producttypename
	, proprietaryname
	, proprietarynamesuffix
	, nonproprietaryname
	, dosageformname
	, routename
	, pack.startmarketingdate
	, pack.endmarketingdate
	, marketingcategoryname
	, applicationnumber
	, labelername
	, substancename
	, active_numerator_strength
	, active_ingred_unit
	, pharm_classes
	, deaschedule
	, pack.ndc_exclude_flag
	, listing_record_certified_through
	, ndcpackagecode
	, packagedescription
	, sample_package
FROM {{source('fda_excluded','fda_excluded_package')}} AS pack
LEFT JOIN {{source('fda_excluded','fda_excluded_product')}} AS prod
	ON pack.productid = prod.productid