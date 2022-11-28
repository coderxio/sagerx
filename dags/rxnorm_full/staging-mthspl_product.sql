/* staging.mthspl_product (DP) */
DROP TABLE IF EXISTS staging.mthspl_product CASCADE;

CREATE TABLE staging.mthspl_product (
	rxcui 					VARCHAR(8) NOT NULL,
	name 					TEXT,
	tty 					VARCHAR(20),
	rxaui 					VARCHAR(20),
	ndc 					VARCHAR(20),
	active					BOOLEAN,
	prescribable			BOOLEAN,
	PRIMARY KEY(rxaui)
);

INSERT INTO staging.mthspl_product
SELECT
	product.rxcui AS rxcui
	, product.str AS name
	, product.tty AS tty
	, product.rxaui AS rxaui
	, product.code AS ndc
	, CASE WHEN product.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN product.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso product
WHERE product.tty = 'DP'
	AND product.sab = 'MTHSPL';