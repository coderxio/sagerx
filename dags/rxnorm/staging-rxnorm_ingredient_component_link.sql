/* staging.rxnorm_ingredient_component_link */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_component_link;

CREATE TABLE staging.rxnorm_ingredient_component_link (
    ingredient_rxcui				varchar(8) NOT NULL,
    ingredient_component_rxcui		varchar(8) NOT NULL,
	PRIMARY KEY(ingredient_rxcui, ingredient_component_rxcui)
);

INSERT INTO staging.rxnorm_ingredient_component_link
SELECT DISTINCT
	ingredient.rxcui AS ingredient_rxcui
	, case when ingredient_component.rxcui is null then ingredient.rxcui else ingredient_component.rxcui end ingredient_component_rxcui
from datasource.rxnorm_rxnconso ingredient
left join datasource.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = ingredient.rxcui and rxnrel.rela = 'has_part'
left join datasource.rxnorm_rxnconso ingredient_component on rxnrel.rxcui1 = ingredient_component.rxcui and ingredient_component.tty = 'IN'
where ingredient.tty in('IN', 'MIN')
	and ingredient.sab = 'RXNORM'
	and ingredient_component.sab = 'RXNORM';
