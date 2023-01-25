/* flatfile.rxnorm_clinical_product_to_ingredient_strength */
CREATE OR REPLACE VIEW flatfile.rxnorm_clinical_product_to_ingredient_strength
AS 
    SELECT
        rcp.rxcui as clinical_product_rxcui
        , rcp.name as clinical_product_name
        , rcp.tty as clinical_product_tty
        , rcpc.rxcui as clinical_product_component_rxcui
        , rcpc.name as clinical_product_compnent_name
        , rcpc.tty as clinical_product_component_tty
        , rdf.rxcui as dose_form_rxcui
        , rdf.name as dose_form_name
        , rdf.tty as dose_form_tty
        , ri.rxcui as ingredient_rxcui
        , ri.name as ingredient_name
        , ri.tty as ingredient_tty
        , ric.rxcui as ingredient_component_rxcui
        , ric.name as ingredient_component_name
        , ric.tty as ingredient_component_tty
        , ris.rxcui as ingredient_strength_rxcui
        , ris.name as ingredient_strength_name
        , ris.numerator_value as strength_numerator_value
        , ris.numerator_unit as strength_numerator_unit
        , ris.denominator_value as strength_denominator_value
        , ris.denominator_unit as strength_denominator_unit
        , ris.text as strength_text
        , rcp.active
        , rcp.prescribable
    FROM staging.rxnorm_clinical_product rcp 
    LEFT JOIN staging.rxnorm_clinical_product_component_link rcpcl 
        ON rcp.rxcui = rcpcl.clinical_product_rxcui 
    LEFT JOIN staging.rxnorm_clinical_product_component rcpc 
        ON rcpcl.clinical_product_component_rxcui = rcpc.rxcui 
    LEFT JOIN staging.rxnorm_dose_form rdf 
        ON rcpc.dose_form_rxcui = rdf.rxcui 
    LEFT JOIN staging.rxnorm_ingredient ri 
        ON rcpc.ingredient_rxcui = ri.rxcui 
    LEFT JOIN staging.rxnorm_ingredient_component_link ricl 
        ON ri.rxcui = ricl.ingredient_rxcui 
    LEFT JOIN staging.rxnorm_ingredient_component ric 
        ON ricl.ingredient_component_rxcui = ric.rxcui 
    LEFT JOIN staging.rxnorm_ingredient_strength_link risl 
        ON rcpc.rxcui = risl.clinical_product_component_rxcui 
        AND ric.rxcui = risl.ingredient_component_rxcui 
    LEFT JOIN staging.rxnorm_ingredient_strength ris 
        ON risl.ingredient_strength_rxcui = ris.rxcui;
