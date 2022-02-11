/* flatfile.rxnorm_clinical_product_to_ndc */
CREATE OR REPLACE VIEW flatfile.rxnorm_clinical_product_to_ndc
AS 
    SELECT
        rcp.clinical_product_rxcui
        , clinical_product_name
        , clinical_product_tty
        , rcpc.clinical_product_component_rxcui
        , clinical_product_compnent_name
        , clinical_product_component_tty
        , rdf.dose_form_rxcui
        , dose_form_name
        , dose_form_tty
        , ri.ingredient_rxcui
        , ingredient_name
        , ingredient_tty
        , brand_product_rxcui
        , ndc
        , rcp.active
        , rcp.prescribable
    FROM staging.rxnorm_clinical_product rcp 
    LEFT JOIN staging.rxnorm_clinical_product_component_link rcpcl 
        ON rcp.clinical_product_rxcui = rcpcl.clinical_product_rxcui 
    LEFT JOIN staging.rxnorm_clinical_product_component rcpc 
        ON rcpcl.clinical_product_component_rxcui = rcpc.clinical_product_component_rxcui 
    LEFT JOIN staging.rxnorm_dose_form rdf 
        ON rcpc.dose_form_rxcui = rdf.dose_form_rxcui 
    LEFT JOIN staging.rxnorm_ingredient ri 
        ON rcpc.ingredient_rxcui = ri.ingredient_rxcui 
    LEFT JOIN staging.rxnorm_ndc rn
        ON rcp.clinical_product_rxcui = rn.clinical_product_rxcui
