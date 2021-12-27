/* staging.rxnorm_clincial_product_component (SCD) */
DROP TABLE IF EXISTS staging.rxnorm_clinical_product_component CASCADE;

CREATE TABLE staging.rxnorm_clinical_product_component (
    clinical_product_component_rxcui VARCHAR(8) NOT NULL,
    clinical_product_compnent_name   TEXT,
    clinical_product_component_tty   VARCHAR(20),
	ingredient_rxcui				 VARCHAR(8),
	dose_form_rxcui					 VARCHAR(8),
	PRIMARY KEY(clinical_product_component_rxcui)
);

INSERT INTO staging.rxnorm_clinical_product_component
WITH cte AS (
	SELECT
	sq.*,
	ROW_NUMBER() OVER(PARTITION BY product_component_rxcui ORDER BY ingredient_tty DESC) AS rn
	FROM (
		SELECT
			product_component.rxcui AS product_component_rxcui
			, product_component.str AS product_component_name
			, product_component.tty AS product_component_tty
			, ingredient.rxcui AS ingredient_rxcui
			, ingredient.str AS ingredient_name
			, ingredient.tty AS ingredient_tty
		FROM datasource.rxnorm_rxnconso product_component
		INNER JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product_component.rxcui AND rxnrel.rela = 'has_ingredients'
		INNER JOIN datasource.rxnorm_rxnconso ingredient
			ON rxnrel.rxcui1 = ingredient.rxcui
			AND ingredient.tty = 'MIN'
			AND ingredient.sab = 'RXNORM'
		WHERE product_component.tty = 'SCD'
			AND product_component.sab = 'RXNORM'

		UNION ALL

		SELECT
			product_component.rxcui AS product_component_rxcui
			, product_component.str AS product_component_name
			, product_component.tty AS product_component_tty
			, ingredient.rxcui AS ingredient_rxcui
			, ingredient.str AS ingredient_name
			, ingredient.tty AS ingredient_tty
		FROM datasource.rxnorm_rxnconso product_component
		INNER JOIN datasource.rxnorm_rxnrel scdc_rxnrel ON scdc_rxnrel.rxcui2 = product_component.rxcui AND scdc_rxnrel.rela = 'consists_of'
		INNER JOIN datasource.rxnorm_rxnconso scdc ON scdc_rxnrel.rxcui1 = scdc.rxcui
		INNER JOIN datasource.rxnorm_rxnrel ingredient_rxnrel ON ingredient_rxnrel.rxcui2 = scdc.rxcui AND ingredient_rxnrel.rela = 'has_ingredient'
		INNER JOIN datasource.rxnorm_rxnconso ingredient
			ON ingredient_rxnrel.rxcui1 = ingredient.rxcui
			AND ingredient.tty = 'IN'
			AND ingredient.sab = 'RXNORM'
		WHERE product_component.tty = 'SCD'
			AND product_component.sab = 'RXNORM'
	) sq
)
SELECT DISTINCT
	CASE WHEN product_component.rxcui IS NULL THEN product.rxcui ELSE product_component.rxcui END clinical_product_component_rxcui
	, CASE WHEN product_component.str IS NULL THEN product.str ELSE product_component.str END clinical_product_compnent_name 
	, CASE WHEN product_component.tty IS NULL THEN product.tty ELSE product_component.tty END clinical_product_component_tty
	, cte.ingredient_rxcui AS ingredient_rxcui
	, dose_form_rxnrel.rxcui1 AS dose_form_rxcui
FROM datasource.rxnorm_rxnconso product
LEFT JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui AND rxnrel.rela = 'contains'
LEFT JOIN datasource.rxnorm_rxnconso product_component
	ON rxnrel.rxcui1 = product_component.rxcui
    AND product_component.tty = 'SCD'
    AND product_component.sab = 'RXNORM'
LEFT JOIN cte 
	ON cte.product_component_rxcui = CASE WHEN product_component.rxcui IS NULL THEN product.rxcui ELSE product_component.rxcui END
	AND cte.rn < 2
LEFT JOIN datasource.rxnorm_rxnrel dose_form_rxnrel
	ON dose_form_rxnrel.rxcui2 = CASE WHEN product_component.rxcui IS NULL THEN product.rxcui ELSE product_component.rxcui END
	AND dose_form_rxnrel.rela = 'has_dose_form'
	AND dose_form_rxnrel.sab = 'RXNORM'
WHERE product.tty IN('SCD', 'GPCK')
	AND product.sab = 'RXNORM';
	