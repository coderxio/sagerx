/* staging.fda_enforcement_ndc_json */
CREATE TABLE IF NOT EXISTS staging.fda_enforcement_ndc_json (
	recall_number	TEXT,
	ndc11 			TEXT,
	ndc9 			TEXT,
	app_num			TEXT,
	PRIMARY KEY(recall_number, ndc11)
);

INSERT INTO staging.fda_enforcement_ndc_json
SELECT
	fdae.recall_number
	, ndc_to_11(ndc.id_value #>> '{}') AS ndc11
	, LEFT(ndc_to_11(ndc.id_value #>> '{}'),9) AS ndc9
	, app_num.id_value #>> '{}' AS app_num
FROM datasource.fda_enforcement fdae
	, json_array_elements(openfda->'package_ndc') with ordinality ndc(id_value, line)
	, json_array_elements(openfda->'application_number') with ordinality app_num(id_value, line)
WHERE ndc_to_11(ndc.id_value #>> '{}') IS NOT NULL
ON CONFLICT DO NOTHING
;