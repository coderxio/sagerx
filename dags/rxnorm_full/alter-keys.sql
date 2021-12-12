/* staging.rxnorm_ndc */
ALTER TABLE staging.rxnorm_ndc
    DROP CONSTRAINT IF EXISTS fk_clinical_product,
    ADD CONSTRAINT fk_clinical_product
        FOREIGN KEY (clinical_product_rxcui)
            REFERENCES staging.rxnorm_clinical_product(clinical_product_rxcui);
ALTER TABLE staging.rxnorm_ndc     
    DROP CONSTRAINT IF EXISTS fk_brand_product,
    ADD CONSTRAINT fk_brand_product
        FOREIGN KEY (brand_product_rxcui)
            REFERENCES staging.rxnorm_brand_product(brand_product_rxcui);
