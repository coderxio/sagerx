/* staging.rxnorm_clincial_product_component (SCD) */
DROP TABLE IF EXISTS staging.rxnorm_clinical_product_component;

CREATE TABLE staging.rxnorm_clinical_product_component (
    clinical_product_component_rxcui VARCHAR(8) PRIMARY KEY,
    clinical_product_compnent_name   TEXT,
    clinical_product_component_tty   VARCHAR(20),
	ingredient_rxcui				 VARCHAR(8)
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
		INNER JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product_component.rxcui and rxnrel.rela = 'has_ingredients'
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
		INNER JOIN datasource.rxnorm_rxnrel scdc_rxnrel ON scdc_rxnrel.rxcui2 = product_component.rxcui and scdc_rxnrel.rela = 'consists_of'
		INNER JOIN datasource.rxnorm_rxnconso scdc ON scdc_rxnrel.rxcui1 = scdc.rxcui
		INNER JOIN datasource.rxnorm_rxnrel ingredient_rxnrel ON ingredient_rxnrel.rxcui2 = scdc.rxcui and ingredient_rxnrel.rela = 'has_ingredient'
		INNER JOIN datasource.rxnorm_rxnconso ingredient
			ON ingredient_rxnrel.rxcui1 = ingredient.rxcui
			AND ingredient.tty = 'IN'
			AND ingredient.sab = 'RXNORM'
		WHERE product_component.tty = 'SCD'
			AND product_component.sab = 'RXNORM'
	) sq
)
SELECT DISTINCT
	case when product_component.rxcui is null then product.rxcui else product_component.rxcui end clinical_product_component_rxcui
	, case when product_component.str is null then product.str else product_component.str end clinical_product_compnent_name 
	, case when product_component.tty is null then product.tty else product_component.tty end clinical_product_component_tty
	, cte.ingredient_rxcui AS ingredient_rxcui
from datasource.rxnorm_rxnconso product
left join datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'contains'
left join datasource.rxnorm_rxnconso product_component
    on rxnrel.rxcui1 = product_component.rxcui
    and product_component.tty = 'SCD'
    and product_component.sab = 'RXNORM'
LEFT JOIN cte ON cte.product_component_rxcui = case when product_component.rxcui is null then product.rxcui else product_component.rxcui end
	and cte.rn < 2
where product.tty in('SCD', 'GPCK')
	and product.sab = 'RXNORM';
	