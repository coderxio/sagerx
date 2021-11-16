/* staging.rxnorm_brand_product_component_link */
DROP TABLE IF EXISTS staging.rxnorm_brand_product_component_link;

CREATE TABLE staging.rxnorm_brand_product_component_link (
    brand_product_rxcui          	varchar(8) NOT NULL,
    brand_product_component_rxcui	varchar(8) NOT NULL,
	PRIMARY KEY(brand_product_rxcui, brand_product_component_rxcui)
);

INSERT INTO staging.rxnorm_brand_product_component_link
SELECT DISTINCT
	product.rxcui AS brand_product_rxcui
	, case when product_component.rxcui is null then product.rxcui else product_component.rxcui end AS brand_product_component_rxcui
from datasource.rxnconso product
left join datasource.rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join datasource.rxnconso product_component on rxnrel.rxcui1 = product_component.rxcui and product_component.tty = 'SBD'
where product.tty in('SBD', 'BPCK');
