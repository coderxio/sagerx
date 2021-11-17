/* staging.rxnorm_brand_product_component (SBD) */
DROP TABLE IF EXISTS staging.rxnorm_brand_product_component;

CREATE TABLE staging.rxnorm_brand_product_component (
    brand_product_component_rxcui       varchar(8) NOT NULL,
    brand_product_component_name		TEXT,
	brand_product_component_tty			varchar(20),
	PRIMARY KEY(brand_product_component_rxcui)
);

INSERT INTO staging.rxnorm_brand_product_component
SELECT DISTINCT
	case when product_component.rxcui is null then product.rxcui else product_component.rxcui end brand_product_component_rxcui
	, case when product_component.str is null then product.str else product_component.str end brand_product_component_name
	, case when product_component.tty is null then product.tty else product_component.tty end brand_product_component_tty
from datasource.rxnconso product
left join datasource.rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join datasource.rxnconso product_component on rxnrel.rxcui1 = product_component.rxcui and product_component.tty = 'SBD'
where product.tty in('SBD', 'BPCK');
