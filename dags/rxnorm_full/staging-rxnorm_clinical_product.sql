/* staging.rxnorm_clinical_product (SCD/GPCK) */
DROP TABLE IF EXISTS staging.rxnorm_clinical_product CASCADE;

CREATE TABLE staging.rxnorm_clinical_product (
	rxcui 					VARCHAR(8) NOT NULL,
	name 					TEXT,
	tty 					VARCHAR(20),
	active					BOOLEAN,
	prescribable			BOOLEAN,
	PRIMARY KEY(rxcui)
);

INSERT INTO staging.rxnorm_clinical_product
SELECT
	product.rxcui AS rxcui
	, product.str AS name
	, product.tty AS tty
	, CASE WHEN product.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN product.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso product
WHERE product.tty IN('SCD', 'GPCK')
	AND product.sab = 'RXNORM';
