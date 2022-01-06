/*  staging.rxnorm_ingredient(in/min) */
DROP TABLE IF EXISTS staging.rxnorm_ingredient CASCADE;

CREATE TABLE staging.rxnorm_ingredient (
    ingredient_rxcui		VARCHAR(8) NOT NULL,
    ingredient_name 		TEXT,
	ingredient_tty		VARCHAR(20),
	active				BOOLEAN,
	prescribable		BOOLEAN,
	PRIMARY KEY(ingredient_rxcui)
);

INSERT INTO staging.rxnorm_ingredient
SELECT
	ingredient.rxcui ingredient_rxcui
	, ingredient.str ingredient_name
	, ingredient.tty ingredient_tty
	, CASE WHEN ingredient.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN ingredient.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso ingredient
WHERE ingredient.tty IN('IN', 'MIN')
	AND ingredient.sab = 'RXNORM';
