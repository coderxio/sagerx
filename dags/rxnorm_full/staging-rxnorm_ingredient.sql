/*  staging.rxnorm_ingredient(in/min) */
DROP TABLE IF EXISTS staging.rxnorm_ingredient;

CREATE TABLE staging.rxnorm_ingredient (
    ingredient_rxcui		varchar(8) NOT NULL,
    ingredient_name 		TEXT,
	ingredient_tty		varchar(20),
	PRIMARY KEY(ingredient_rxcui)
);

INSERT INTO staging.rxnorm_ingredient
SELECT
	ingredient.rxcui ingredient_rxcui
	, ingredient.str ingredient_name
	, ingredient.tty ingredient_tty
from datasource.rxnorm_rxnconso ingredient
where ingredient.tty in('IN', 'MIN')
	and ingredient.sab = 'RXNORM';
