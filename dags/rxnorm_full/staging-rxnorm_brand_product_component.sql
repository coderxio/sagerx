/* staging.rxnorm_brand_product_component (SBD) */
DROP TABLE IF EXISTS staging.rxnorm_brand_product_component;

CREATE TABLE staging.rxnorm_brand_product_component (
    brand_product_component_rxcui       VARCHAR(8) NOT NULL,
    brand_product_component_name		TEXT,
	brand_product_component_tty			VARCHAR(20),
    clinical_product_component_rxcui	VARCHAR(8) NOT NULL,
    brand_rxcui    						VARCHAR(8), -- NOTE: brand_product_component SCDs will have NULL for brand_rxcui
	PRIMARY KEY(brand_product_component_rxcui)
);

INSERT INTO staging.rxnorm_brand_product_component
SELECT DISTINCT
	CASE WHEN product.tty = 'SBD' THEN product.rxcui ELSE product_component.rxcui END brand_product_component_rxcui
	, CASE WHEN product.tty = 'SBD' THEN product.str ELSE product_component.str END brand_product_component_name
	, CASE WHEN product.tty = 'SBD' THEN product.tty ELSE product_component.tty END brand_product_component_tty
	, CASE WHEN product_component.tty = 'SCD' THEN product_component.rxcui ELSE clinical_product_component.rxcui END clinical_product_component_rxcui
	, brand.rxcui AS brand_rxcui
FROM datasource.rxnorm_rxnconso product
LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'contains'
LEFT JOIN datasource.rxnorm_rxnconso product_component
	ON rxnrel.rxcui1 = product_component.rxcui
	AND product_component.tty IN ('SBD', 'SCD') -- NOTE: BPCKs can contain SBDs AND SCDs
	AND product_component.sab = 'RXNORM'
LEFT JOIN datasource.rxnorm_rxnrel rxnrel2 
	ON rxnrel2.rxcui2 = CASE WHEN product_component.rxcui IS NULL THEN product.rxcui ELSE product_component.rxcui END 
	AND rxnrel2.rela = 'tradename_of'
LEFT JOIN datasource.rxnorm_rxnconso clinical_product_component
    ON rxnrel2.rxcui1 = clinical_product_component.rxcui
    AND clinical_product_component.tty = 'SCD'
    AND clinical_product_component.sab = 'RXNORM'
LEFT JOIN datasource.rxnorm_rxnrel rxnrel3 
	ON rxnrel3.rxcui2 = CASE WHEN product_component.rxcui IS NULL THEN product.rxcui ELSE product_component.rxcui END 
	AND rxnrel3.rela = 'has_ingredient'
LEFT JOIN datasource.rxnorm_rxnconso brand
	ON rxnrel3.rxcui1 = brand.rxcui
	AND brand.tty = 'BN'
	AND brand.sab = 'RXNORM'
where product.tty in('SBD', 'BPCK')
	AND product.sab = 'RXNORM';
