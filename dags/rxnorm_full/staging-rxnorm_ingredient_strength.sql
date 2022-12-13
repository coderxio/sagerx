/* staging.rxnorm_ingredient_strength */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_strength CASCADE;

CREATE TABLE staging.rxnorm_ingredient_strength (
	rxcui					varchar(8),
	name					TEXT,
	numerator_value			TEXT,
	numerator_unit			TEXT,
	denominator_value		TEXT,
	denominator_unit		TEXT,
	text					TEXT,
	active					BOOLEAN,
	prescribable			BOOLEAN,
	PRIMARY KEY (rxcui)
);

INSERT INTO staging.rxnorm_ingredient_strength
SELECT
	ingredient_strength.rxcui AS rxcui
	, ingredient_strength.str AS name
	, numerator_value.atv AS numerator_value
	, numerator_unit.atv AS numerator_unit
	, denominator_value.atv AS denominator_value
	, denominator_unit.atv AS denominator_unit
	, text.atv AS text
	, CASE WHEN ingredient_strength.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN ingredient_strength.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
from datasource.rxnorm_rxnconso ingredient_strength
left join datasource.rxnorm_rxnsat numerator_value on numerator_value.rxcui = ingredient_strength.rxcui and numerator_value.atn = 'RXN_BOSS_STRENGTH_NUM_VALUE'
left join datasource.rxnorm_rxnsat numerator_unit on numerator_unit.rxcui = ingredient_strength.rxcui and numerator_unit.atn = 'RXN_BOSS_STRENGTH_NUM_UNIT'
left join datasource.rxnorm_rxnsat denominator_value on denominator_value.rxcui = ingredient_strength.rxcui and denominator_value.atn = 'RXN_BOSS_STRENGTH_DENOM_VALUE'
left join datasource.rxnorm_rxnsat denominator_unit on denominator_unit.rxcui = ingredient_strength.rxcui and denominator_unit.atn = 'RXN_BOSS_STRENGTH_DENOM_UNIT'
left join datasource.rxnorm_rxnsat text on text.rxcui = ingredient_strength.rxcui and text.atn = 'RXN_STRENGTH'
where ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM';
