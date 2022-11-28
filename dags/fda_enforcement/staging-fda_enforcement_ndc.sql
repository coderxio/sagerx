/* staging.fda_enforcement_ndc  */
DROP TABLE IF EXISTS staging.fda_enforcement_ndc;

CREATE TABLE staging.fda_enforcement_ndc (
	recall_number	TEXT,
	ndc11 			TEXT
);

INSERT INTO staging.fda_enforcement_ndc
SELECT recall_number
	, ndc_to_11((regexp_matches(product_description, 'NDC (\d+-\d+-\d+)', 'g'))[1]) AS ndc11
FROM datasource.fda_enforcement;