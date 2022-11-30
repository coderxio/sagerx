/* flatfile.rxnorm_ndc_to_product */
CREATE OR REPLACE VIEW flatfile.rxnorm_ndc_to_product
AS 
    SELECT
        ndc
        , coalesce(rbp.rxcui, rcp.rxcui, null) AS rxcui
        , coalesce(rbp.name, rcp.name, null) AS name
        , coalesce(rbp.tty, rcp.tty, null) AS tty
    FROM staging.rxnorm_ndc ndc
    LEFT JOIN staging.rxnorm_clinical_product rcp 
        ON ndc.clinical_product_rxcui = rcp.rxcui
    LEFT JOIN staging.rxnorm_brand_product rbp
        ON ndc.brand_product_rxcui = rbp.rxcui
    WHERE ndc.active AND ndc.prescribable