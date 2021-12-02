/* staging.rxnorm_clincial_product_component (SCD) */
DROP TABLE IF EXISTS staging.rxnorm_clinical_product_component;

CREATE TABLE staging.rxnorm_clinical_product_component (
    clinical_product_component_rxcui varchar(8) PRIMARY KEY,
    clinical_product_compnent_name   TEXT,
    clinical_product_component_tty   varchar(20)
);

INSERT INTO staging.rxnorm_clinical_product_component
SELECT DISTINCT
	case when product_component.rxcui is null then product.rxcui else product_component.rxcui end clinical_product_component_rxcui
	, case when product_component.str is null then product.str else product_component.str end clinical_product_compnent_name 
	, case when product_component.tty is null then product.tty else product_component.tty end clinical_product_component_tty
from datasource.rxnorm_rxnconso product
left join datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join datasource.rxnorm_rxnconso product_component
    on rxnrel.rxcui1 = product_component.rxcui
    and product_component.tty = 'SCD'
    and product_component.sab = 'RXNORM'
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM';
