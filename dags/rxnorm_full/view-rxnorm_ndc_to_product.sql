/* flatfile.rxnorm_ndc_to_product */
CREATE OR REPLACE VIEW flatfile.rxnorm_ndc_to_product
AS 
    select
        ndc
        , coalesce(rbp.rxcui, rcp.rxcui, null) as rxcui
        , coalesce(rbp.name, rcp.name, null) as name
        , coalesce(rbp.tty, rcp.tty, null) as tty
    from staging.rxnorm_ndc ndc
    left join staging.rxnorm_clinical_product rcp 
        on ndc.clinical_product_rxcui = rcp.rxcui
    left join staging.rxnorm_brand_product rbp
        on ndc.brand_product_rxcui = rbp.rxcui
    where ndc.active and ndc.prescribable