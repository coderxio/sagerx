/* staging.rxnorm_ingredient_strength */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_strength CASCADE;

CREATE TABLE staging.rxnorm_ingredient_strength (
	ingredient_strength_rxcui			varchar(8),
	ingredient_strength_name			TEXT,
	strength_numerator_value			TEXT,
	strength_numerator_unit				TEXT,
	strength_denominator_value			TEXT,
	strength_denominator_unit			TEXT,
	strength_text						TEXT,
	strength_active_ingredient			TEXT,
	strength_active_moeity				TEXT,
	strength_from						TEXT,
	PRIMARY KEY (ingredient_strength_rxcui)
);

INSERT INTO staging.rxnorm_ingredient_strength
SELECT
	ingredient_strength.rxcui AS ingredient_strength_rxcui
	, ingredient_strength.str AS ingredient_strength_name
	, strength_numerator_value.atv AS strength_numerator_value
	, strength_numerator_unit.atv AS strength_numerator_unit
	, strength_denominator_value.atv AS strength_denominator_value
	, strength_denominator_unit.atv AS strength_denominator_unit
	, strength_text.atv AS strength_text
	, strength_active_ingredient.atv AS strength_active_ingredient
	, strength_active_moeity.atv AS strength_active_moeity
	, CASE
		WHEN strength_from.atv = 'AI' THEN 'ACTIVE_INGREDIENT'
		WHEN strength_from.atv = 'AM' THEN 'ACTIVE_MOEITY'
		ELSE NULL
	  END AS strength_from
from datasource.rxnorm_rxnconso ingredient_strength
left join datasource.rxnorm_rxnsat strength_numerator_value on strength_numerator_value.rxcui = ingredient_strength.rxcui and strength_numerator_value.atn = 'RXN_BOSS_STRENGTH_NUM_VALUE'
left join datasource.rxnorm_rxnsat strength_numerator_unit on strength_numerator_unit.rxcui = ingredient_strength.rxcui and strength_numerator_unit.atn = 'RXN_BOSS_STRENGTH_NUM_UNIT'
left join datasource.rxnorm_rxnsat strength_denominator_value on strength_denominator_value.rxcui = ingredient_strength.rxcui and strength_denominator_value.atn = 'RXN_BOSS_STRENGTH_DENOM_VALUE'
left join datasource.rxnorm_rxnsat strength_denominator_unit on strength_denominator_unit.rxcui = ingredient_strength.rxcui and strength_denominator_unit.atn = 'RXN_BOSS_STRENGTH_DENOM_UNIT'
left join datasource.rxnorm_rxnsat strength_text on strength_text.rxcui = ingredient_strength.rxcui and strength_text.atn = 'RXN_STRENGTH'
left join datasource.rxnorm_rxnsat strength_active_ingredient on strength_active_ingredient.rxcui = ingredient_strength.rxcui and strength_active_ingredient.atn = 'RXN_AI'
left join datasource.rxnorm_rxnsat strength_active_moeity on strength_active_moeity.rxcui = ingredient_strength.rxcui and strength_active_moeity.atn = 'RXN_AM'
left join datasource.rxnorm_rxnsat strength_from on strength_from.rxcui = ingredient_strength.rxcui and strength_from.atn = 'RXN_BOSS_FROM'
where ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM';
