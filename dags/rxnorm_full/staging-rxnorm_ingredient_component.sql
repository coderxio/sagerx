/* staging.rxnorm_ingredient_component (IN) */
DROP TABLE IF EXISTS staging.rxnorm_ingredient_component CASCADE;

CREATE TABLE staging.rxnorm_ingredient_component (
    rxcui					VARCHAR(8) NOT NULL,
    name 					TEXT,
	tty						VARCHAR(20),
	active					BOOLEAN,
	prescribable			BOOLEAN,
	PRIMARY KEY(rxcui)
);

INSERT INTO staging.rxnorm_ingredient_component
WITH cte AS (
	SELECT
		rxnrel.rxcui2 AS ingredient_rxcui
		, ingredient_component.rxcui AS rxcui
		, ingredient_component.str AS name
		, ingredient_component.tty AS tty
		, ingredient_component.suppress
		, ingredient_component.cvf
	FROM
		datasource.rxnorm_rxnrel rxnrel
	INNER JOIN datasource.rxnorm_rxnconso ingredient_component
		ON rxnrel.rxcui1 = ingredient_component.rxcui
	WHERE rxnrel.rela = 'has_part'
		AND ingredient_component.tty = 'IN'
		AND ingredient_component.sab = 'RXNORM'
)
SELECT DISTINCT
	CASE WHEN cte.rxcui IS NULL THEN ingredient.rxcui ELSE cte.rxcui END rxcui
	, CASE WHEN cte.name IS NULL THEN ingredient.str ELSE cte.name END name
	, CASE WHEN cte.tty IS NULL THEN ingredient.tty ELSE cte.tty END tty
	, CASE WHEN 
		CASE WHEN cte.rxcui IS NULL THEN ingredient.suppress ELSE cte.suppress END = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN 
		CASE WHEN cte.rxcui IS NULL THEN ingredient.cvf ELSE cte.cvf END = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso ingredient
LEFT JOIN cte ON ingredient.rxcui = cte.ingredient_rxcui
WHERE ingredient.tty IN('IN', 'MIN')
	AND ingredient.sab = 'RXNORM';
