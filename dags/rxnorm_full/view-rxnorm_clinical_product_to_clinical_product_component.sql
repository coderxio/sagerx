/* flatfile.rxnorm_clinical_product_to_clinical_product_component */
CREATE OR REPLACE VIEW flatfile.rxnorm_clinical_product_to_clinical_product_component
AS 
    SELECT
        rcp.rxcui as clinical_product_rxcui
        , rcp.name as clinical_product_name
        , rcp.tty as clinical_product_tty
        , rcpc.rxcui as clinical_product_component_rxcui
        , rcpc.name as clinical_product_compnent_name
        , rcpc.tty as clinical_product_component_tty
        , rcp.active
        , rcp.prescribable
    FROM staging.rxnorm_clinical_product rcp 
    LEFT JOIN staging.rxnorm_clinical_product_component_link rcpcl 
        ON rcp.rxcui = rcpcl.clinical_product_rxcui 
    LEFT JOIN staging.rxnorm_clinical_product_component rcpc 
        ON rcpcl.clinical_product_component_rxcui = rcpc.rxcui 
