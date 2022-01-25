/* flatfile.all_ndc*/
CREATE OR REPLACE VIEW flatfile.all_ndc
AS 
    WITH cte AS (
        SELECT DISTINCT sq.* FROM
        (
            SELECT ndc
            FROM staging.rxnorm_ndc

            UNION ALL

            SELECT ndc11 AS ndc
            FROM staging.fda_ndc
        ) sq
    )
    SELECT
        cte.ndc
        , rxnorm_clinical_product_rxcui
        , rxnorm_brand_product_rxcui
        , fda_productid
    FROM cte
    LEFT JOIN (SELECT
        ndc
        , clinical_product_rxcui AS rxnorm_clinical_product_rxcui
        , brand_product_rxcui AS rxnorm_brand_product_rxcui
    FROM staging.rxnorm_ndc) rxnorm ON cte.ndc = rxnorm.ndc
    LEFT JOIN (SELECT
        ndc11
        , productid AS fda_productid
    FROM staging.fda_ndc) fda ON cte.ndc = fda.ndc11
