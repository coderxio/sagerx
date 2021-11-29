/* stagnig.rxnorm_brand (BN) */
DROP TABLE IF EXISTS staging.rxnorm_brand;

CREATE TABLE staging.rxnorm_brand (
    brand_rxcui         varchar(8) NOT NULL,
    brand_name			TEXT,
	brand_tty			varchar(20),
	PRIMARY KEY(brand_rxcui)
);

INSERT INTO staging.rxnorm_brand
SELECT DISTINCT
	brand.rxcui AS brand_rxcui
	, brand.str AS brand_name
	, brand.tty AS brand_tty
from datasource.rxnorm_rxnconso product
left join datasource.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'has_ingredient'
left join datasource.rxnorm_rxnconso brand on rxnrel.rxcui1 = brand.rxcui and brand.tty = 'BN'
where product.tty = 'SBD'
	and product.sab = 'RXNORM'
	and brand.sab = 'RXNORM';
