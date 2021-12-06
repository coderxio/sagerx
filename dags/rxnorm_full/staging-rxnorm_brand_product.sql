/* staging.rxnorm_brand_product */
DROP TABLE IF EXISTS staging.rxnorm_brand_product;

CREATE TABLE staging.rxnorm_brand_product (
    brand_product_rxcui         VARCHAR(8) NOT NULL,
    brand_product_name			TEXT,
	brand_product_tty			VARCHAR(20),
    clinical_product_rxcui      VARCHAR(8) NOT NULL,
	PRIMARY KEY(brand_product_rxcui)
);

INSERT INTO staging.rxnorm_brand_product
SELECT
	product.rxcui AS brand_product_rxcui
	, product.str AS brand_product_name
	, product.tty AS brand_product_tty
	, clinical_product.rxcui AS clinical_product_rxcui
FROM datasource.rxnorm_rxnconso product
LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'tradename_of'
LEFT JOIN datasource.rxnorm_rxnconso clinical_product
	ON rxnrel.rxcui1 = clinical_product.rxcui
	AND clinical_product.tty IN ('SCD', 'GPCK')
	AND clinical_product.sab = 'RXNORM'
WHERE product.tty IN('SBD', 'BPCK')
	AND product.sab = 'RXNORM';
