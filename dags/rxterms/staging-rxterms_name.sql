/* staging.rxterms_name  */
DROP TABLE IF EXISTS staging.rxterms_name;

CREATE TABLE staging.rxterms_name (
	name 					TEXT,
	synonyms 				TEXT
);

INSERT INTO staging.rxterms_name
SELECT DISTINCT
    display_name AS name
    , display_name_synonym AS synonyms
FROM datasource.rxterms
WHERE suppress_for IS NULL 
    AND is_retired IS NULL;
