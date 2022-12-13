/* staging.rxnorm_brand_product */
DROP TABLE IF EXISTS staging.rxnorm_brand_product CASCADE;

CREATE TABLE staging.rxnorm_brand_product (
    rxcui         VARCHAR(8) NOT NULL,
    name			TEXT,
	tty			VARCHAR(20),
    clinical_product_rxcui      VARCHAR(8) NOT NULL,
	active						BOOLEAN,
	prescribable				BOOLEAN,
	PRIMARY KEY(rxcui)
);

INSERT INTO staging.rxnorm_brand_product
SELECT
	product.rxcui AS rxcui
	, product.str AS name
	, product.tty AS tty
	, clinical_product.rxcui AS clinical_product_rxcui
	, CASE WHEN product.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN product.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso product
LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'tradename_of'
LEFT JOIN datasource.rxnorm_rxnconso clinical_product
	ON rxnrel.rxcui1 = clinical_product.rxcui
	AND clinical_product.tty IN ('SCD', 'GPCK')
	AND clinical_product.sab = 'RXNORM'
WHERE product.tty IN('SBD', 'BPCK')
	AND product.sab = 'RXNORM';
