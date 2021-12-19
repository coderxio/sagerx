/* staging.rxnorm_clinical_product_component_link */
DROP TABLE IF EXISTS staging.rxnorm_clinical_product_component_link CASCADE;

CREATE TABLE staging.rxnorm_clinical_product_component_link (
    clinical_product_rxcui           varchar(8) NOT NULL,
    clinical_product_component_rxcui varchar(8) NOT NULL,
	PRIMARY KEY(clinical_product_rxcui, clinical_product_component_rxcui)
);

INSERT INTO staging.rxnorm_clinical_product_component_link
SELECT DISTINCT
	product.rxcui AS clinical_product_rxcui
	, case when product_component.rxcui is null then product.rxcui 
                            else product_component.rxcui 
    end AS rxnorm_clinical_product_component_rxcui
from datasource.rxnorm_rxnconso product
left join datasource.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join datasource.rxnorm_rxnconso product_component
    on rxnrel.rxcui1 = product_component.rxcui
    and product_component.tty = 'SCD'
    and product_component.sab = 'RXNORM'
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM';
