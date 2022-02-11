/* staging.rxnorm_dose_form */
DROP TABLE IF EXISTS staging.rxnorm_dose_form CASCADE;

CREATE TABLE staging.rxnorm_dose_form (
	dose_form_rxcui			VARCHAR(8) NOT NULL,
    dose_form_name 			TEXT,
	dose_form_tty			VARCHAR(20),
	active					BOOLEAN,
	prescribable			BOOLEAN,
	PRIMARY KEY(dose_form_rxcui)
);

INSERT INTO staging.rxnorm_dose_form
SELECT
	dose_form.rxcui dose_form_rxcui
	, dose_form.str dose_form_name
	, dose_form.tty dose_form_tty
	, CASE WHEN dose_form.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN dose_form.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso dose_form
WHERE dose_form.tty = 'DF'
	AND dose_form.sab = 'RXNORM';
