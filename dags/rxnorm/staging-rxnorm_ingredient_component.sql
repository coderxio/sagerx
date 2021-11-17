/* staging.rxnorm_ingredient_component (IN) */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_component;

CREATE TABLE staging.rxnorm_ingredient_component (
    ingredient_component_rxcui		varchar(8) NOT NULL,
    ingredient_component_name 		TEXT,
	ingredient_component_tty		varchar(20),
	PRIMARY KEY(ingredient_component_rxcui)
);

INSERT INTO staging.rxnorm_ingredient_component
SELECT DISTINCT
	case when ingredient_component.rxcui is null then ingredient.rxcui else ingredient_component.rxcui END ingredient_component_rxcui
	, case when ingredient_component.str is null then ingredient.str else ingredient_component.str END ingredient_component_name
	, case when ingredient_component.tty is null then ingredient.tty else ingredient_component.tty END ingredient_component_tty
from datasource.rxnconso ingredient
left join datasource.rxnrel on rxnrel.rxcui2 = ingredient.rxcui and rxnrel.rela = 'has_part'
left join datasource.rxnconso ingredient_component on rxnrel.rxcui1 = ingredient_component.rxcui and ingredient_component.tty = 'IN'
where ingredient.tty in('IN', 'MIN');
