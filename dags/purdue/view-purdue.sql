CREATE OR REPLACE VIEW flatfile.purdue
AS 
	WITH enf AS (
		-- UNION RegEx NDCs from description and JSON NDCs from OpenFDA column
		-- NOTE: UNION removes duplicates by default, which is what we want here
		SELECT
			*
		FROM (
			-- first get only the NDCs from combined RegEx and JSON enforcement report data
			SELECT 
				recall_number
				, ndc11
				, ndc9
			FROM staging.fda_enforcement_ndc_regex
			
			UNION
			
			SELECT
				recall_number
				, ndc11
				, ndc9
			FROM staging.fda_enforcement_ndc_json
		) enf_ndcs
		-- then, join NDCs with application numbers, where they exist from JSON data
		LEFT JOIN (
			-- get distinct NDC11 -> application numbers so as to not blow up granularity
			SELECT
				ndc11
				, app_num
			FROM staging.fda_enforcement_ndc_json
			GROUP BY
				ndc11
				, app_num
		) json_app_num
			ON enf_ndcs.ndc11 = json_app_num.ndc11
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
		-- if FDA NDC Directory has an application number, use that...
		-- otherwise, use the application number from the enforcement report JSON
		, COALESCE(fda.applicationnumber, enf.app_num) AS application_number
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