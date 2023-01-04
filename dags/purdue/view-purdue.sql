CREATE OR REPLACE VIEW flatfile.purdue
AS 
	WITH enf AS (
		-- UNION RegEx NDCs from description and JSON NDCs from OpenFDA column
		SELECT 
			*
		FROM staging.fda_enforcement_ndc_regex
		
		UNION
		
		SELECT
			*
		FROM staging.fda_enforcement_ndc_json
	)
	SELECT
		ndc.ndc AS ndc11
		, LEFT(ndc.ndc, 9) AS ndc9
		, ndc.product_rxcui
		, ndc.product_name
		, ndc.product_tty
		, ndc.clinical_product_rxcui
		, ndc.clinical_product_name
		, ndc.clinical_product_tty
		, ing.ingredient_rxcui AS active_ingredient_rxcui
		, ing.ingredient_name AS active_ingredient_name
		, ing.ingredient_tty AS active_ingredient_tty
		, fda.applicationnumber AS application_number
		, enf.recall_number
		, spl.inactive_ingredient_unii
		, spl.inactive_ingredient_rxcui
		, spl.inactive_ingredient_name
		, spl.inactive_ingredient_tty
	-- RxNorm NDCs that have one of the ingredients in the WHERE clause
	FROM flatfile.rxnorm_ndc_to_product ndc
	-- clinical product and related active ingredients for each NDC
	LEFT JOIN flatfile.rxnorm_clinical_product_to_ingredient ing
		ON ing.clinical_product_rxcui = ndc.clinical_product_rxcui
	-- FDA enforcement reports (UNION of RegEx and JSON) joined on NDC9
	LEFT JOIN enf
		ON enf.ndc9 = LEFT(ndc.ndc, 9)
	-- DailyMed SPL inactive ingredients joined on NDC9
	LEFT JOIN flatfile.mthspl_product_to_inactive_ingredient spl
		ON spl.ndc9 = LEFT(ndc.ndc, 9)
	-- FDA NDC Directory joined on NDC11 to pull in application number
	LEFT JOIN staging.fda_ndc fda
		ON fda.ndc11 = ndc.ndc
    WHERE ing.ingredient_name in (
        'risperidone'
        , 'adalimumab'
        , 'lidocaine'
        , 'carbamazepine'
        , 'phenytoin'
        , 'midazolam'
        , 'valproate'
        , 'tacrolimus'
        , 'amoxicillin'
        , 'hydrocortisone'
        , 'cetirizine'
        , 'pertuzumab'
        , 'methylphenidate'
        , 'erythromycin'
        , 'gabapentin'
        , 'lopinavir / ritonavir'
        , 'levothyroxine'
        , 'albuterol'
        )
;