/* flatfile.rxnorm_ndc_to_product */
CREATE OR REPLACE VIEW flatfile.rxnorm_ndc_to_product
AS 
    SELECT DISTINCT
        ndc
		, COALESCE(rbp.rxcui, rcp.rxcui, null) AS product_rxcui
		, COALESCE(rbp.name, rcp.name, null) AS product_name
		, COALESCE(rbp.tty, rcp.tty, null) AS product_tty
		, rcp.rxcui AS clinical_product_rxcui
		, rcp.name AS clinical_product_name
		, rcp.tty AS clinical_product_tty
    FROM staging.rxnorm_ndc ndc
    LEFT JOIN staging.rxnorm_clinical_product rcp 
        ON ndc.clinical_product_rxcui = rcp.rxcui
    LEFT JOIN staging.rxnorm_brand_product rbp
        ON ndc.brand_product_rxcui = rbp.rxcui