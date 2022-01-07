/* staging.fda_substance  */
DROP TABLE IF EXISTS staging.fda_substance;

CREATE TABLE staging.fda_substance (
	productid					TEXT NOT NULL,
	substance_line 				TEXT NOT NULL,
	substancename 				TEXT,
	active_numerator_strength	TEXT,
	active_ingred_unit 			TEXT,
	PRIMARY KEY (productid, substance_line)
);

INSERT INTO staging.fda_substance
SELECT
	substance.productid
	, substance.rn AS substance_line
	, substance.token AS substancename
	, strength.token AS active_numerator_strength
	, unit.token AS active_ingred_unit
FROM (
	SELECT DISTINCT
		prod.productid
		, ROW_NUMBER() OVER (PARTITION BY prod.productid) AS rn
		, arr.token
	FROM datasource.fda_ndc_product prod
	, UNNEST(string_to_array(prod.substancename, '; ')) arr(token)
) substance
LEFT JOIN (
	SELECT DISTINCT
		prod.productid
		, ROW_NUMBER() OVER (PARTITION BY prod.productid) AS rn
		, arr.token
	FROM datasource.fda_ndc_product prod
	, UNNEST(string_to_array(prod.active_numerator_strength, '; ')) arr(token)
) strength ON substance.productid = strength.productid AND substance.rn = strength.rn
LEFT JOIN (
	SELECT DISTINCT
		prod.productid
		, ROW_NUMBER() OVER (PARTITION BY prod.productid) AS rn
		, arr.token
	FROM datasource.fda_ndc_product prod
	, UNNEST(string_to_array(prod.active_ingred_unit, '; ')) arr(token)
) unit ON substance.productid = unit.productid AND substance.rn = unit.rn;
