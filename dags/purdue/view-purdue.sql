CREATE OR REPLACE VIEW flatfile.purdue
AS 
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
		, fda.recall_number
		, spl.inactive_ingredient_unii
		, spl.inactive_ingredient_rxcui
		, spl.inactive_ingredient_name
		, spl.inactive_ingredient_tty
	FROM flatfile.rxnorm_ndc_to_product ndc
	LEFT JOIN flatfile.rxnorm_clinical_product_to_ingredient ing
		ON ing.clinical_product_rxcui = ndc.clinical_product_rxcui
	LEFT JOIN staging.fda_enforcement_ndc fda
		ON fda.ndc9 = LEFT(ndc.ndc, 9)
	LEFT JOIN flatfile.mthspl_product_to_inactive_ingredient spl
		ON spl.ndc9 = LEFT(ndc.ndc, 9)