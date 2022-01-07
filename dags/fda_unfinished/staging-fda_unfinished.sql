/* staging.fda_unfinished  */
DROP TABLE IF EXISTS staging.fda_unfinished;

CREATE TABLE staging.fda_unfinished (
	ndc11 								TEXT,
	productid 							TEXT,
	productndc 							TEXT,
	producttypename						TEXT,
	nonproprietaryname					TEXT,
	dosageformname						TEXT,
	startmarketingdate					TEXT,
	endmarketingdate					TEXT,
	marketingcategoryname				TEXT,
	labelername							TEXT,
	substancename						TEXT,
	active_numerator_strength			TEXT,
	active_ingred_unit					TEXT,
	deaschedule							TEXT,
	listing_record_certified_through	TEXT,
	ndcpackagecode						TEXT,
	packagedescription					TEXT
);

INSERT INTO staging.fda_unfinished
SELECT
	ndc_to_11(pack.ndcpackagecode) AS ndc11
	, pack.productid
	, pack.productndc
	, producttypename
	, nonproprietaryname
	, dosageformname
	, pack.startmarketingdate
	, pack.endmarketingdate
	, marketingcategoryname
	, labelername
	, substancename
	, active_numerator_strength
	, active_ingred_unit
	, deaschedule
	, listing_record_certified_through
	, ndcpackagecode
	, packagedescription
FROM datasource.fda_unfinished_package pack
LEFT JOIN datasource.fda_unfinished_product prod
	ON pack.productid = prod.productid;
