/* flatfile.rxnorm_clinical_product_to_ingredient_strength */
CREATE OR REPLACE VIEW flatfile.rxnorm_clinical_product_to_ingredient_strength
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
        , ric.ingredient_component_rxcui
        , ingredient_component_name
        , ingredient_component_tty
        , ris.ingredient_strength_rxcui
        , ingredient_strength_name
        , strength_numerator_value
        , strength_numerator_unit
        , strength_denominator_value
        , strength_denominator_unit
        , strength_text
        , strength_active_ingredient
        , strength_active_moeity
        , strength_from
    FROM staging.rxnorm_clinical_product rcp 
    LEFT JOIN staging.rxnorm_clinical_product_component_link rcpcl 
        ON rcp.clinical_product_rxcui = rcpcl.clinical_product_rxcui 
    LEFT JOIN staging.rxnorm_clinical_product_component rcpc 
        ON rcpcl.clinical_product_component_rxcui = rcpc.clinical_product_component_rxcui 
    LEFT JOIN staging.rxnorm_dose_form rdf 
        ON rcpc.dose_form_rxcui = rdf.dose_form_rxcui 
    LEFT JOIN staging.rxnorm_ingredient ri 
        ON rcpc.ingredient_rxcui = ri.ingredient_rxcui 
    LEFT JOIN staging.rxnorm_ingredient_component_link ricl 
        ON ri.ingredient_rxcui = ricl.ingredient_rxcui 
    LEFT JOIN staging.rxnorm_ingredient_component ric 
        ON ricl.ingredient_component_rxcui = ric.ingredient_component_rxcui 
    LEFT JOIN staging.rxnorm_ingredient_strength_link risl 
        ON rcpc.clinical_product_component_rxcui = risl.clinical_product_component_rxcui 
        AND ric.ingredient_component_rxcui = risl.ingredient_component_rxcui 
    LEFT JOIN staging.rxnorm_ingredient_strength ris 
        ON risl.ingredient_strength_rxcui = ris.ingredient_strength_rxcui;
