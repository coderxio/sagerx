/* staging.fda_enforcement_ndc  */
CREATE TABLE IF NOT EXISTS staging.fda_enforcement_ndc (
	recall_number	TEXT,
	ndc11 			TEXT,
	ndc9 			TEXT,
	PRIMARY KEY(recall_number, ndc11)
);

INSERT INTO staging.fda_enforcement_ndc
SELECT recall_number
	, ndc_to_11((regexp_matches(product_description, '(\m\d{1,5}-\d{1,4}-\d{1,2}\M|\m\d{11}\M)', 'g'))[1]) AS ndc11
	, LEFT(ndc_to_11((regexp_matches(product_description, '(\m\d{1,5}-\d{1,4}-\d{1,2}\M|\m\d{11}\M)', 'g'))[1]), 9) AS ndc9
FROM datasource.fda_enforcement
ON CONFLICT DO NOTHING
;