/*  staging.rxnorm_ingredient(in/min) */
DROP TABLE IF EXISTS staging.rxnorm_ingredient CASCADE;

CREATE TABLE staging.rxnorm_ingredient (
    rxcui				VARCHAR(8) NOT NULL,
    name 				TEXT,
	tty					VARCHAR(20),
	active				BOOLEAN,
	prescribable		BOOLEAN,
	PRIMARY KEY(rxcui)
);

INSERT INTO staging.rxnorm_ingredient
SELECT
	ingredient.rxcui rxcui
	, ingredient.str name
	, ingredient.tty tty
	, CASE WHEN ingredient.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN ingredient.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso ingredient
WHERE ingredient.tty IN('IN', 'MIN')
	AND ingredient.sab = 'RXNORM';
