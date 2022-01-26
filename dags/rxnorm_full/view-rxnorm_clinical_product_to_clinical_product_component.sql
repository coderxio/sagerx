/* flatfile.rxnorm_clinical_product_to_clinical_product_component */
CREATE OR REPLACE VIEW flatfile.rxnorm_clinical_product_to_clinical_product_component
AS 
    SELECT
        rcp.clinical_product_rxcui
        , clinical_product_name
        , clinical_product_tty
        , rcpc.clinical_product_component_rxcui
        , clinical_product_compnent_name
        , clinical_product_component_tty
        , rcp.active
        , rcp.prescribable
    FROM staging.rxnorm_clinical_product rcp 
    LEFT JOIN staging.rxnorm_clinical_product_component_link rcpcl 
        ON rcp.clinical_product_rxcui = rcpcl.clinical_product_rxcui 
    LEFT JOIN staging.rxnorm_clinical_product_component rcpc 
        ON rcpcl.clinical_product_component_rxcui = rcpc.clinical_product_component_rxcui 
