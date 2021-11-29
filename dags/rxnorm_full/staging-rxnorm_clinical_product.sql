/* staging.rxnorm_clinical_product (SCD/GPCK) */
DROP TABLE IF EXISTS staging.rxnorm_clinical_product CASCADE;

CREATE TABLE staging.rxnorm_clinical_product (
	clinical_product_rxcui 	varchar(8) PRIMARY KEY,
	clinical_product_name 	TEXT,
	clinical_product_tty 	varchar(20)
);

INSERT INTO staging.rxnorm_clinical_product
SELECT
	product.rxcui AS clinical_product_rxcui
	, product.str AS clinical_product_name
	, product.tty AS clinical_product_tty
from datasource.rxnorm_rxnconso product
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM';
