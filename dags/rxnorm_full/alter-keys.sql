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

/* staging.rxnorm_clinical_product_component_link */
ALTER TABLE staging.rxnorm_clinical_product_component_link
    DROP CONSTRAINT IF EXISTS fk_clinical_product,
    ADD CONSTRAINT fk_clinical_product
        FOREIGN KEY (clinical_product_rxcui)
            REFERENCES staging.rxnorm_clinical_product(clinical_product_rxcui);
ALTER TABLE staging.rxnorm_clinical_product_component_link     
    DROP CONSTRAINT IF EXISTS fk_clinical_product_component,
    ADD CONSTRAINT fk_clinical_product_component
        FOREIGN KEY (clinical_product_component_rxcui)
            REFERENCES staging.rxnorm_clinical_product_component(clinical_product_component_rxcui);

/* staging.rxnorm_clinical_product_component */
ALTER TABLE staging.rxnorm_clinical_product_component
    DROP CONSTRAINT IF EXISTS fk_ingredient,
    ADD CONSTRAINT fk_ingredient
        FOREIGN KEY (ingredient_rxcui)
            REFERENCES staging.rxnorm_ingredient(ingredient_rxcui);

/* staging.rxnorm_brand_product */
ALTER TABLE staging.rxnorm_brand_product
    DROP CONSTRAINT IF EXISTS fk_clinical_product,
    ADD CONSTRAINT fk_clinical_product
        FOREIGN KEY (clinical_product_rxcui)
            REFERENCES staging.rxnorm_clinical_product(clinical_product_rxcui);

/* staging.rxnorm_brand_product_component_link */
ALTER TABLE staging.rxnorm_brand_product_component_link
    DROP CONSTRAINT IF EXISTS fk_brand_product,
    ADD CONSTRAINT fk_brand_product
        FOREIGN KEY (brand_product_rxcui)
            REFERENCES staging.rxnorm_brand_product(brand_product_rxcui);
ALTER TABLE staging.rxnorm_brand_product_component_link
    DROP CONSTRAINT IF EXISTS fk_brand_product_component,
    ADD CONSTRAINT fk_brand_product_component
        FOREIGN KEY (brand_product_component_rxcui)
            REFERENCES staging.rxnorm_brand_product_component(brand_product_component_rxcui);

/* staging.rxnorm_brand_product_component */
ALTER TABLE staging.rxnorm_brand_product_component
    DROP CONSTRAINT IF EXISTS fk_brand,
    ADD CONSTRAINT fk_brand
        FOREIGN KEY (brand_rxcui)
            REFERENCES staging.rxnorm_brand(brand_rxcui);
ALTER TABLE staging.rxnorm_brand_product_component
    DROP CONSTRAINT IF EXISTS fk_clinical_product_component,
    ADD CONSTRAINT fk_clinical_product_component
        FOREIGN KEY (clinical_product_component_rxcui)
            REFERENCES staging.rxnorm_clinical_product_component(clinical_product_component_rxcui);

/* staging.rxnorm_brand */
ALTER TABLE staging.rxnorm_brand
    DROP CONSTRAINT IF EXISTS fk_ingredient,
    ADD CONSTRAINT fk_ingredient
        FOREIGN KEY (ingredient_rxcui)
            REFERENCES staging.rxnorm_ingredient(ingredient_rxcui);

/* staging.rxnorm_ingredient_component_link */
ALTER TABLE staging.rxnorm_ingredient_component_link
    DROP CONSTRAINT IF EXISTS fk_ingredient,
    ADD CONSTRAINT fk_ingredient
        FOREIGN KEY (ingredient_rxcui)
            REFERENCES staging.rxnorm_ingredient(ingredient_rxcui);
ALTER TABLE staging.rxnorm_ingredient_component_link     
    DROP CONSTRAINT IF EXISTS fk_ingredient_component,
    ADD CONSTRAINT fk_ingredient_component
        FOREIGN KEY (ingredient_component_rxcui)
            REFERENCES staging.rxnorm_ingredient_component(ingredient_component_rxcui);

/* staging.rxnorm_ingredient_strength_link */
ALTER TABLE staging.rxnorm_ingredient_strength_link
    DROP CONSTRAINT IF EXISTS fk_clinical_product_component,
    ADD CONSTRAINT fk_clinical_product_component
        FOREIGN KEY (clinical_product_component_rxcui)
            REFERENCES staging.rxnorm_clinical_product_component(clinical_product_component_rxcui);
ALTER TABLE staging.rxnorm_ingredient_strength_link
    DROP CONSTRAINT IF EXISTS fk_ingredient_component,
    ADD CONSTRAINT fk_ingredient_component
        FOREIGN KEY (ingredient_component_rxcui)
            REFERENCES staging.rxnorm_ingredient_component(ingredient_component_rxcui);
ALTER TABLE staging.rxnorm_ingredient_strength_link
    DROP CONSTRAINT IF EXISTS fk_ingredient_strength,
    ADD CONSTRAINT fk_ingredient_strength
        FOREIGN KEY (ingredient_strength_rxcui)
            REFERENCES staging.rxnorm_ingredient_strength(ingredient_strength_rxcui);
