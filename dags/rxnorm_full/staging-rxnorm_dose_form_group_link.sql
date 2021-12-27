/* staging.rxnorm_dose_form_group_link */
DROP TABLE IF EXISTS staging.rxnorm_dose_form_group_link CASCADE;

CREATE TABLE staging.rxnorm_dose_form_group_link (
	dose_form_rxcui			VARCHAR(8) NOT NULL,
	dose_form_group_rxcui	VARCHAR(8) NOT NULL,
	PRIMARY KEY(dose_form_rxcui, dose_form_group_rxcui)
);

INSERT INTO staging.rxnorm_dose_form_group_link
SELECT DISTINCT
	dose_form.rxcui dose_form_rxcui
	, rxnrel.rxcui1 dose_form_group_rxcui
FROM datasource.rxnorm_rxnconso dose_form
INNER JOIN datasource.rxnorm_rxnrel rxnrel
	ON rxnrel.rxcui2 = dose_form.rxcui
	AND rxnrel.rela = 'isa'
	AND rxnrel.sab = 'RXNORM'
WHERE dose_form.tty = 'DF'
	AND dose_form.sab = 'RXNORM';
