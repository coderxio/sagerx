/* staging.rxnorm_ingredient_component_link */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_component_link CASCADE;

CREATE TABLE staging.rxnorm_ingredient_component_link (
    ingredient_rxcui				varchar(8) NOT NULL,
    ingredient_component_rxcui		varchar(8) NOT NULL,
	PRIMARY KEY(ingredient_rxcui, ingredient_component_rxcui)
);

INSERT INTO staging.rxnorm_ingredient_component_link
WITH cte AS (
	SELECT
		rxnrel.rxcui2 AS ingredient_rxcui
		, ingredient_component.rxcui AS ingredient_component_rxcui
		, ingredient_component.str AS ingredient_component_name
		, ingredient_component.tty AS ingredient_component_tty
	FROM
		datasource.rxnorm_rxnrel rxnrel
	INNER JOIN datasource.rxnorm_rxnconso ingredient_component
		ON rxnrel.rxcui1 = ingredient_component.rxcui
	WHERE rxnrel.rela = 'has_part'
		AND ingredient_component.tty = 'IN'
		AND ingredient_component.sab = 'RXNORM'
)
SELECT DISTINCT
	ingredient.rxcui AS ingredient_rxcui
	, CASE WHEN cte.ingredient_component_rxcui IS NULL THEN ingredient.rxcui ELSE cte.ingredient_component_rxcui END ingredient_component_rxcui
FROM datasource.rxnorm_rxnconso ingredient
LEFT JOIN cte ON ingredient.rxcui = cte.ingredient_rxcui
WHERE ingredient.tty IN('IN', 'MIN')
	AND ingredient.sab = 'RXNORM';
