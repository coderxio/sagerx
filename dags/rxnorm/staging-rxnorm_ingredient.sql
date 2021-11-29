/*  staging.rxnorm_ingredient(in/min) */
DROP TABLE IF EXISTS staging.rxnorm_ingredient;

CREATE TABLE staging.rxnorm_ingredient (
    rxnorm_ingredient_rxcui		varchar(8) NOT NULL,
    rxnorm_ingredient_name 		TEXT,
	rxnorm_ingredient_tty		varchar(20),
	PRIMARY KEY(rxnorm_ingredient_rxcui)
);

INSERT INTO staging.rxnorm_ingredient
SELECT
	ingredient.rxcui rxnorm_ingredient_rxcui
	, ingredient.str rxnorm_ingredient_name
	, ingredient.tty rxnorm_ingredient_tty
from datasource.rxnorm_rxnconso ingredient
where ingredient.tty in('IN', 'MIN')
	and ingredient.sab = 'RXNORM';
