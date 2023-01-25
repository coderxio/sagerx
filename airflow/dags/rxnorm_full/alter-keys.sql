/* staging.rxnorm_ndc */
ALTER TABLE staging.rxnorm_ndc
    DROP CONSTRAINT IF EXISTS fk_clinical_product,
    ADD CONSTRAINT fk_clinical_product
        FOREIGN KEY (clinical_product_rxcui)
            REFERENCES staging.rxnorm_clinical_product(rxcui);
ALTER TABLE staging.rxnorm_ndc     
    DROP CONSTRAINT IF EXISTS fk_brand_product,
    ADD CONSTRAINT fk_brand_product
        FOREIGN KEY (brand_product_rxcui)
            REFERENCES staging.rxnorm_brand_product(rxcui);

/* staging.rxnorm_clinical_product_component_link */
ALTER TABLE staging.rxnorm_clinical_product_component_link
    DROP CONSTRAINT IF EXISTS fk_clinical_product,
    ADD CONSTRAINT fk_clinical_product
        FOREIGN KEY (clinical_product_rxcui)
            REFERENCES staging.rxnorm_clinical_product(rxcui);
ALTER TABLE staging.rxnorm_clinical_product_component_link     
    DROP CONSTRAINT IF EXISTS fk_clinical_product_component,
    ADD CONSTRAINT fk_clinical_product_component
        FOREIGN KEY (clinical_product_component_rxcui)
            REFERENCES staging.rxnorm_clinical_product_component(rxcui);

/* staging.rxnorm_clinical_product_component */
ALTER TABLE staging.rxnorm_clinical_product_component
    DROP CONSTRAINT IF EXISTS fk_ingredient,
    ADD CONSTRAINT fk_ingredient
        FOREIGN KEY (ingredient_rxcui)
            REFERENCES staging.rxnorm_ingredient(rxcui);
ALTER TABLE staging.rxnorm_clinical_product_component
    DROP CONSTRAINT IF EXISTS fk_dose_form,
    ADD CONSTRAINT fk_dose_form
        FOREIGN KEY (dose_form_rxcui)
            REFERENCES staging.rxnorm_dose_form(rxcui);

/* staging.rxnorm_brand_product */
ALTER TABLE staging.rxnorm_brand_product
    DROP CONSTRAINT IF EXISTS fk_clinical_product,
    ADD CONSTRAINT fk_clinical_product
        FOREIGN KEY (clinical_product_rxcui)
            REFERENCES staging.rxnorm_clinical_product(rxcui);

/* staging.rxnorm_brand_product_component_link */
ALTER TABLE staging.rxnorm_brand_product_component_link
    DROP CONSTRAINT IF EXISTS fk_brand_product,
    ADD CONSTRAINT fk_brand_product
        FOREIGN KEY (brand_product_rxcui)
            REFERENCES staging.rxnorm_brand_product(rxcui);
ALTER TABLE staging.rxnorm_brand_product_component_link
    DROP CONSTRAINT IF EXISTS fk_brand_product_component,
    ADD CONSTRAINT fk_brand_product_component
        FOREIGN KEY (brand_product_component_rxcui)
            REFERENCES staging.rxnorm_brand_product_component(rxcui);

/* staging.rxnorm_brand_product_component */
ALTER TABLE staging.rxnorm_brand_product_component
    DROP CONSTRAINT IF EXISTS fk_brand,
    ADD CONSTRAINT fk_brand
        FOREIGN KEY (brand_rxcui)
            REFERENCES staging.rxnorm_brand(rxcui);
ALTER TABLE staging.rxnorm_brand_product_component
    DROP CONSTRAINT IF EXISTS fk_clinical_product_component,
    ADD CONSTRAINT fk_clinical_product_component
        FOREIGN KEY (clinical_product_component_rxcui)
            REFERENCES staging.rxnorm_clinical_product_component(rxcui);

/* staging.rxnorm_brand */
ALTER TABLE staging.rxnorm_brand
    DROP CONSTRAINT IF EXISTS fk_ingredient,
    ADD CONSTRAINT fk_ingredient
        FOREIGN KEY (ingredient_rxcui)
            REFERENCES staging.rxnorm_ingredient(rxcui);

/* staging.rxnorm_ingredient_component_link */
ALTER TABLE staging.rxnorm_ingredient_component_link
    DROP CONSTRAINT IF EXISTS fk_ingredient,
    ADD CONSTRAINT fk_ingredient
        FOREIGN KEY (ingredient_rxcui)
            REFERENCES staging.rxnorm_ingredient(rxcui);
ALTER TABLE staging.rxnorm_ingredient_component_link     
    DROP CONSTRAINT IF EXISTS fk_ingredient_component,
    ADD CONSTRAINT fk_ingredient_component
        FOREIGN KEY (ingredient_component_rxcui)
            REFERENCES staging.rxnorm_ingredient_component(rxcui);

/* staging.rxnorm_ingredient_strength_link */
ALTER TABLE staging.rxnorm_ingredient_strength_link
    DROP CONSTRAINT IF EXISTS fk_clinical_product_component,
    ADD CONSTRAINT fk_clinical_product_component
        FOREIGN KEY (clinical_product_component_rxcui)
            REFERENCES staging.rxnorm_clinical_product_component(rxcui);
ALTER TABLE staging.rxnorm_ingredient_strength_link
    DROP CONSTRAINT IF EXISTS fk_ingredient_component,
    ADD CONSTRAINT fk_ingredient_component
        FOREIGN KEY (ingredient_component_rxcui)
            REFERENCES staging.rxnorm_ingredient_component(rxcui);
ALTER TABLE staging.rxnorm_ingredient_strength_link
    DROP CONSTRAINT IF EXISTS fk_ingredient_strength,
    ADD CONSTRAINT fk_ingredient_strength
        FOREIGN KEY (ingredient_strength_rxcui)
            REFERENCES staging.rxnorm_ingredient_strength(rxcui);

/* staging.rxnorm_dose_form_group_link */
ALTER TABLE staging.rxnorm_dose_form_group_link
    DROP CONSTRAINT IF EXISTS fk_dose_form,
    ADD CONSTRAINT fk_dose_form
        FOREIGN KEY (dose_form_rxcui)
            REFERENCES staging.rxnorm_dose_form(rxcui);
ALTER TABLE staging.rxnorm_dose_form_group_link     
    DROP CONSTRAINT IF EXISTS fk_dose_form_group,
    ADD CONSTRAINT fk_dose_form_group
        FOREIGN KEY (dose_form_group_rxcui)
            REFERENCES staging.rxnorm_dose_form_group(rxcui);
