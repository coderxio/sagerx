/* staging.fda_enforcement_ndc_regex */
CREATE TABLE IF NOT EXISTS staging.fda_enforcement_ndc_regex (
	recall_number	TEXT,
	ndc11 			TEXT,
	ndc9 			TEXT,
	PRIMARY KEY(recall_number, ndc11) 
	/*
	NOTE: ndc11 is sometimes not found, resulting in an Airflow error
	ERROR - Failed to execute job 204 for task staging-fda_enforcement_ndc_regex.sql (null value in column "ndc11" of relation "fda_enforcement_ndc_regex" violates not-null constraint
	DETAIL:  Failing row contains (D-1512-2022, null, null).
	*/
);

INSERT INTO staging.fda_enforcement_ndc_regex
SELECT recall_number
	, ndc_to_11((regexp_matches(product_description, '(\m\d{1,5}-\d{1,4}-\d{1,2}\M|\m\d{11}\M)', 'g'))[1]) AS ndc11
	, LEFT(ndc_to_11((regexp_matches(product_description, '(\m\d{1,5}-\d{1,4}-\d{1,2}\M|\m\d{11}\M)', 'g'))[1]), 9) AS ndc9
FROM datasource.fda_enforcement
ON CONFLICT DO NOTHING
;