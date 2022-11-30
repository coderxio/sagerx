/* staging.fda_enforcement_ndc  */
CREATE TABLE IF NOT EXISTS staging.fda_enforcement_ndc (
	recall_number	TEXT,
	ndc11 			TEXT,
	ndc9 			TEXT
);

INSERT INTO staging.fda_enforcement_ndc
SELECT recall_number
	, ndc_to_11((regexp_matches(product_description, 'NDC (\d+-\d+-\d+)', 'g'))[1]) AS ndc11
	, LEFT(ndc_to_11((regexp_matches(product_description, 'NDC (\d+-\d+-\d+)', 'g'))[1]), 9) AS ndc9
FROM datasource.fda_enforcement
ON CONFLICT DO NOTHING
;