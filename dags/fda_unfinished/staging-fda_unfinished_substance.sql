/* staging.fda_unfinished_substance  */
DROP TABLE IF EXISTS staging.fda_unfinished_substance;

CREATE TABLE staging.fda_unfinished_substance (
	productid					TEXT NOT NULL,
	substance_line 				TEXT NOT NULL,
	substancename 				TEXT,
	active_numerator_strength	TEXT,
	active_ingred_unit 			TEXT,
	PRIMARY KEY (productid, substance_line)
);

INSERT INTO staging.fda_unfinished_substance
SELECT DISTINCT
    prod.productid
    , ROW_NUMBER() OVER (PARTITION BY prod.productid) AS rn
    , arr.substancename
    , arr.active_numerator_strength
    , arr.active_ingred_unit
FROM datasource.fda_ndc_product prod
    , UNNEST(string_to_array(prod.substancename, '; ')
            ,string_to_array(prod.active_numerator_strength, '; ')
            ,string_to_array(prod.active_ingred_unit, '; ')
            ) arr(substancename,active_numerator_strength,active_ingred_unit)