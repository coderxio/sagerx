/* staging.rxnorm_ingredient_strength_link */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_strength_link CASCADE;

CREATE TABLE staging.rxnorm_ingredient_strength_link (
	clinical_product_component_rxcui	varchar(8),
	ingredient_component_rxcui			varchar(8),
	ingredient_strength_rxcui			varchar(8),
	PRIMARY KEY (clinical_product_component_rxcui,
				 ingredient_component_rxcui,
				ingredient_strength_rxcui)
);

INSERT INTO staging.rxnorm_ingredient_strength_link
SELECT DISTINCT
	product_component.rxcui AS clinical_product_component_rxcui
	, ingredient.rxcui AS ingredient_component_rxcui
	, ingredient_strength.rxcui AS ingredient_strength_rxcui
from datasource.rxnorm_rxnconso ingredient_strength
inner join datasource.rxnorm_rxnrel has_ingredient on has_ingredient.rxcui2 = ingredient_strength.rxcui and has_ingredient.rela = 'has_ingredient'
inner join datasource.rxnorm_rxnconso ingredient
	on ingredient.rxcui = has_ingredient.rxcui1
	and ingredient.tty = 'IN'
	and ingredient.sab = 'RXNORM'
inner join datasource.rxnorm_rxnrel constitutes on constitutes.rxcui2 = ingredient_strength.rxcui and constitutes.rela = 'constitutes'
inner join datasource.rxnorm_rxnconso product_component
	on product_component.rxcui = constitutes.rxcui1
	and product_component.tty = 'SCD'
	and product_component.sab = 'RXNORM'
where ingredient_strength.tty = 'SCDC'
	and ingredient_strength.sab = 'RXNORM';
