/* staging.rxnorm_brand_product */
DROP TABLE IF EXISTS staging.rxnorm_brand_product;

CREATE TABLE staging.rxnorm_brand_product (
    brand_product_rxcui         varchar(8) NOT NULL,
    brand_product_name			TEXT,
	brand_product_tty			varchar(20),
	PRIMARY KEY(brand_product_rxcui)
);

INSERT INTO staging.rxnorm_brand_product
SELECT
	product.rxcui AS brand_product_rxcui
	, product.str AS brand_product_name
	, product.tty AS brand_product_tty
from datasource.rxnconso product
where product.tty in('SBD', 'BPCK');
