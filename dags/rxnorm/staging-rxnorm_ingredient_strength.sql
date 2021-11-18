/* staging.rxnorm_ingredient_strength */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_strength;

CREATE TABLE staging.rxnorm_ingredient_strength (
	clinical_product_component_rxcui	varchar(8),
	ingredient_component_rxcui			varchar(8),
	ingredient_strength_rxcui			varchar(8),
	ingredient_strength_name			TEXT,
	strength_numerator_value			TEXT,
	strength_numerator_unit				TEXT,
	strength_denominator_value			TEXT,
	strength_denominator_unit			TEXT,
	strength_text						TEXT,
	strength_active_ingreident			TEXT,
	strength_active_moeity				TEXT,
	strength_from						TEXT,
	PRIMARY KEY (clinical_product_component_rxcui,
				 ingredient_component_rxcui,
				ingredient_strength_rxcui)
);

INSERT INTO staging.rxnorm_ingredient_strength
SELECT
	product_component.rxcui AS clinical_product_component_rxcui
	, ingredient.rxcui AS ingredient_component_rxcui
	, ingredient_strength.rxcui AS ingreident_strength_rxcui
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
from datasource.rxnconso ingredient_strength
inner join datasource.rxnrel has_ingredient on has_ingredient.rxcui2 = ingredient_strength.rxcui and has_ingredient.rela = 'has_ingredient'
inner join datasource.rxnconso ingredient on ingredient.rxcui = has_ingredient.rxcui1 and ingredient.tty = 'IN'
inner join datasource.rxnrel constitutes on constitutes.rxcui2 = ingredient_strength.rxcui and constitutes.rela = 'constitutes'
inner join datasource.rxnconso product_component on product_component.rxcui = constitutes.rxcui1 and product_component.tty = 'SCD'
left join datasource.rxnsat strength_numerator_value on strength_numerator_value.rxcui = ingredient_strength.rxcui and strength_numerator_value.atn = 'RXN_BOSS_STRENGTH_NUM_VALUE'
left join datasource.rxnsat strength_numerator_unit on strength_numerator_unit.rxcui = ingredient_strength.rxcui and strength_numerator_unit.atn = 'RXN_BOSS_STRENGTH_NUM_UNIT'
left join datasource.rxnsat strength_denominator_value on strength_denominator_value.rxcui = ingredient_strength.rxcui and strength_denominator_value.atn = 'RXN_BOSS_STRENGTH_DENOM_VALUE'
left join datasource.rxnsat strength_denominator_unit on strength_denominator_unit.rxcui = ingredient_strength.rxcui and strength_denominator_unit.atn = 'RXN_BOSS_STRENGTH_DENOM_UNIT'
left join datasource.rxnsat strength_text on strength_text.rxcui = ingredient_strength.rxcui and strength_text.atn = 'RXN_STRENGTH'
left join datasource.rxnsat strength_active_ingredient on strength_active_ingredient.rxcui = ingredient_strength.rxcui and strength_active_ingredient.atn = 'RXN_BOSS_AI'
left join datasource.rxnsat strength_active_moeity on strength_active_moeity.rxcui = ingredient_strength.rxcui and strength_active_moeity.atn = 'RXN_BOSS_AM'
left join datasource.rxnsat strength_from on strength_from.rxcui = ingredient_strength.rxcui and strength_from.atn = 'RXN_BOSS_FROM'
where ingredient_strength.tty = 'SCDC';