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
WITH cte AS (
	SELECT
		product.rxcui AS product_rxcui
		, product.str AS product_name
		, product.tty AS product_tty
		, brand.rxcui AS brand_rxcui
	FROM datasource.rxnorm_rxnconso product
	INNER JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'has_ingredient'
	INNER JOIN datasource.rxnorm_rxnconso brand
		ON rxnrel.rxcui1 = brand.rxcui
		AND brand.tty = 'BN'
		AND brand.sab = 'RXNORM'
	WHERE product.tty = 'SBD'
		AND product.sab = 'RXNORM'
	/*
	UNION ALL
	
	-- BPCK doesn't really have a "brand" - links to SBD brands and breaks granularity
	-- NOTE: seems like RxNorm uses a SY as the name in [Brackets] for BPCKs
	SELECT
		product.rxcui AS product_rxcui
		, product.str AS product_name
		, product.tty AS product_tty
		, brand.rxcui AS brand_rxcui
	FROM datasource.rxnorm_rxnconso product
	INNER JOIN datasource.rxnorm_rxnrel rxnrel_sbd ON rxnrel_sbd.rxcui2 = product.rxcui AND rxnrel_sbd.rela = 'contains'
	INNER JOIN datasource.rxnorm_rxnrel rxnrel_sbdf ON rxnrel_sbdf.rxcui2 = rxnrel_sbd.rxcui1 AND rxnrel_sbdf.rela = 'isa'
	INNER JOIN datasource.rxnorm_rxnrel rxnrel_bn ON rxnrel_bn.rxcui2 = rxnrel_sbdf.rxcui1 AND rxnrel_bn.rela = 'has_ingredient'
	INNER JOIN datasource.rxnorm_rxnconso brand
		ON rxnrel_bn.rxcui1 = brand.rxcui
		AND brand.tty = 'BN'
		AND brand.sab = 'RXNORM'
	WHERE product.tty = 'BPCK'
		AND product.sab = 'RXNORM'
	*/
)
SELECT
	product.rxcui AS brand_product_rxcui
	, product.str AS brand_product_name
	, product.tty AS brand_product_tty
	, clinical_product.rxcui AS clinical_product_rxcui
	, cte.brand_rxcui AS brand_rxcui
FROM datasource.rxnorm_rxnconso product
LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'tradename_of'
LEFT JOIN datasource.rxnorm_rxnconso clinical_product
	ON rxnrel.rxcui1 = clinical_product.rxcui
	AND clinical_product.tty IN ('SCD', 'GPCK')
	AND clinical_product.sab = 'RXNORM'
LEFT JOIN cte ON product.rxcui = cte.product_rxcui
WHERE product.tty IN('SBD', 'BPCK')
	AND product.sab = 'RXNORM';
