/* staging.rxterms_strength  */
DROP TABLE IF EXISTS staging.rxterms_strength;

CREATE TABLE staging.rxterms_strength (
    rxcui       TEXT,
	name 	    TEXT,
	strength 	TEXT
);

INSERT INTO staging.rxterms_strength
SELECT DISTINCT
    rxcui
    , display_name AS name
    , CONCAT(strength, ' ', new_dose_form) AS strength
FROM datasource.rxterms
WHERE suppress_for IS NULL 
    AND is_retired IS NULL;
