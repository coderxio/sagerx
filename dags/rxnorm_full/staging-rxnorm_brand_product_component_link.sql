/* staging.rxnorm_brand_product_component_link */
DROP TABLE IF EXISTS staging.rxnorm_brand_product_component_link;

CREATE TABLE staging.rxnorm_brand_product_component_link (
    brand_product_rxcui          	VARCHAR(8) NOT NULL,
    brand_product_component_rxcui	VARCHAR(8) NOT NULL,
	PRIMARY KEY(brand_product_rxcui, brand_product_component_rxcui)
);

INSERT INTO staging.rxnorm_brand_product_component_link
SELECT DISTINCT
	product.rxcui AS brand_product_rxcui
	, CASE WHEN product_component.rxcui IS NULL THEN product.rxcui ELSE product_component.rxcui END AS brand_product_component_rxcui
FROM datasource.rxnorm_rxnconso product
LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'contains'
LEFT JOIN datasource.rxnorm_rxnconso product_component
	ON rxnrel.rxcui1 = product_component.rxcui
	AND product_component.tty IN ('SBD', 'SCD') -- NOTE: BPCKs can contain SBDs AND SCDs
	AND product_component.sab = 'RXNORM'
WHERE product.tty IN ('SBD', 'BPCK')
	AND product.sab = 'RXNORM';
