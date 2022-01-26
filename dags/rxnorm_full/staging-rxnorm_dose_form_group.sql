/* staging.rxnorm_dose_form_group */
DROP TABLE IF EXISTS staging.rxnorm_dose_form_group CASCADE;

CREATE TABLE staging.rxnorm_dose_form_group (
	dose_form_group_rxcui	VARCHAR(8) NOT NULL,
    dose_form_group_name 	TEXT,
	dose_form_group_tty		VARCHAR(20),
	active					BOOLEAN,
	prescribable			BOOLEAN,
	PRIMARY KEY(dose_form_group_rxcui)
);

INSERT INTO staging.rxnorm_dose_form_group
SELECT
	dose_form_group.rxcui dose_form_group_rxcui
	, dose_form_group.str dose_form_group_name
	, dose_form_group.tty dose_form_group_tty
	, CASE WHEN dose_form_group.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN dose_form_group.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso dose_form_group
WHERE dose_form_group.tty = 'DFG'
	AND dose_form_group.sab = 'RXNORM';
