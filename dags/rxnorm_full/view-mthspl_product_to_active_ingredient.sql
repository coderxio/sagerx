/* flatfile.mthspl_product_to_active_ingredient */
CREATE OR REPLACE VIEW flatfile.mthspl_product_to_active_ingredient
AS 
    SELECT DISTINCT
        CONCAT(LPAD(SPLIT_PART(product.ndc,'-', 1), 5, '0'), LPAD(SPLIT_PART(product.ndc,'-', 2), 4, '0')) AS ndc9
        , product.ndc AS ndc
        , product.rxcui AS product_rxcui
        , product.name AS product_name
        , product.tty AS product_tty
        , substance.unii AS active_ingredient_unii
        , substance.rxcui AS active_ingredient_rxcui
        , substance.name AS active_ingredient_name
        , substance.tty AS active_ingredient_tty	
        , product.active AS active
        , product.prescribable AS prescribable
    FROM datasource.rxnorm_rxnrel rxnrel
    INNER JOIN staging.mthspl_substance substance
        ON rxnrel.rxaui1 = substance.rxaui
    INNER JOIN staging.mthspl_product product
        ON rxnrel.rxaui2 = product.rxaui
    WHERE rela = 'has_active_ingredient'