/* staging.rxnorm_ndc */
DROP TABLE IF EXISTS staging.rxnorm_ndc CASCADE;

CREATE TABLE staging.rxnorm_ndc (
    ndc        			 		 varchar(12) PRIMARY KEY,
    clinical_product_rxcui       varchar(8),
    brand_product_rxcui          varchar(8)
);

INSERT INTO staging.rxnorm_ndc
SELECT rxnsat.atv as ndc
	,CASE WHEN product.tty IN ('BPCK','SBD') THEN clinical_product.rxcui
		ELSE rxnsat.rxcui END AS clinical_product_rxcui		
	,CASE WHEN product.tty IN ('BPCK','SBD') THEN rxnsat.rxcui
		ELSE NULL END AS brand_product_rxcui

FROM datasource.rxnorm_rxnsat rxnsat
	INNER JOIN datasource.rxnorm_rxnconso product ON rxnsat.rxaui = product.rxaui
	LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnsat.rxcui = rxnrel.rxcui2 AND rela = 'tradename_of' and product.tty IN ('BPCK','SBD')
	LEFT JOIN datasource.rxnorm_rxnconso clinical_product
		ON rxnrel.rxcui1 = clinical_product.rxcui
		AND clinical_product.tty IN ('SCD','GPCK')
		AND clinical_product.sab = 'RXNORM'
WHERE rxnsat.atn = 'NDC'
	AND product.tty in ('SCD','SBD','GPCK','BPCK')
	AND product.sab = 'RXNORM';
