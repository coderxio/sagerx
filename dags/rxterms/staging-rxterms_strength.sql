/* staging.rxterms_strength  */
DROP TABLE IF EXISTS staging.rxterms_strength;

CREATE TABLE staging.rxterms_strength (
	name 	    TEXT,
	strength 	TEXT,
    rxcui       TEXT
);

INSERT INTO staging.rxterms_strength
SELECT DISTINCT
    display_name
    , CONCAT(strength, ' ', new_dose_form) AS strength
    , rxcui
FROM datasource.rxterms
WHERE suppress_for IS NULL 
    AND is_retired IS NULL;
