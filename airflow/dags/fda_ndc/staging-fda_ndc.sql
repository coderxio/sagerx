/* staging.fda_ndc  */
DROP TABLE IF EXISTS staging.fda_ndc;

CREATE TABLE staging.fda_ndc (
	ndc11 								TEXT,
	productid 							TEXT,
	productndc 							TEXT,
	producttypename						TEXT,
	proprietaryname						TEXT,
	proprietarynamesuffix				TEXT,
	nonproprietaryname					TEXT,
	dosageformname						TEXT,
	routename							TEXT,
	startmarketingdate					TEXT,
	endmarketingdate					TEXT,
	marketingcategoryname				TEXT,
	applicationnumber					TEXT,
	labelername							TEXT,
	substancename						TEXT,
	active_numerator_strength			TEXT,
	active_ingred_unit					TEXT,
	pharm_classes						TEXT,
	deaschedule							TEXT,
	ndc_exclude_flag					TEXT,
	listing_record_certified_through	TEXT,
	ndcpackagecode						TEXT,
	packagedescription					TEXT,
	sample_package						TEXT
);

INSERT INTO staging.fda_ndc
SELECT
	ndc_to_11(pack.ndcpackagecode) AS ndc11
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
FROM datasource.fda_ndc_package pack
LEFT JOIN datasource.fda_ndc_product prod
	ON pack.productid = prod.productid;
