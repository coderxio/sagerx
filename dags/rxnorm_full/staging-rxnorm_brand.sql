/* stagnig.rxnorm_brand (BN) */
DROP TABLE IF EXISTS staging.rxnorm_brand;

CREATE TABLE staging.rxnorm_brand (
    brand_rxcui         VARCHAR(8) NOT NULL,
    brand_name			TEXT,
	brand_tty			VARCHAR(20),
    ingredient_rxcui    VARCHAR(8) NOT NULL,
	PRIMARY KEY(brand_rxcui)
);

INSERT INTO staging.rxnorm_brand
WITH cte AS (
	SELECT
		sq.*
		, ROW_NUMBER() OVER(PARTITION BY brand_rxcui ORDER BY ingredient_tty DESC) AS rn
	FROM (

		SELECT
			brand.rxcui AS brand_rxcui
			, brand.str AS brand_name
			, brand.tty AS brand_tty
			, ingredient.rxcui AS ingredient_rxcui
			, ingredient.str AS ingredient_name
			, ingredient.tty AS ingredient_tty
		FROM datasource.rxnorm_rxnconso brand
		INNER JOIN datasource.rxnorm_rxnrel rxnrel ON rxnrel.rxcui2 = brand.rxcui and rxnrel.rela = 'tradename_of'
		INNER JOIN datasource.rxnorm_rxnconso ingredient
			ON rxnrel.rxcui1 = ingredient.rxcui
			AND ingredient.tty = 'IN'
			AND ingredient.sab = 'RXNORM'
		WHERE brand.tty = 'BN'
			AND brand.sab = 'RXNORM'

		UNION ALL

		SELECT
			brand.rxcui AS brand_rxcui
			, brand.str AS brand_name
			, brand.tty AS brand_tty
			, ingredient.rxcui AS ingredient_rxcui
			, ingredient.str AS ingredient_name
			, ingredient.tty AS ingredient_tty
		FROM datasource.rxnorm_rxnconso brand
		INNER JOIN datasource.rxnorm_rxnrel sbd_rxnrel ON sbd_rxnrel.rxcui2 = brand.rxcui and sbd_rxnrel.rela = 'ingredient_of'
		INNER JOIN datasource.rxnorm_rxnconso sbd
			ON sbd_rxnrel.rxcui1 = sbd.rxcui
			AND sbd.tty = 'SBD'
			AND sbd.sab = 'RXNORM'		
		INNER JOIN datasource.rxnorm_rxnrel scd_rxnrel ON scd_rxnrel.rxcui2 = sbd.rxcui and scd_rxnrel.rela = 'tradename_of'
		INNER JOIN datasource.rxnorm_rxnconso scd
			ON scd_rxnrel.rxcui1 = scd.rxcui
			AND scd.tty = 'SCD'
			AND scd.sab = 'RXNORM'		
		INNER JOIN datasource.rxnorm_rxnrel ingredient_rxnrel ON ingredient_rxnrel.rxcui2 = scd.rxcui and ingredient_rxnrel.rela = 'has_ingredients'
		LEFT JOIN datasource.rxnorm_rxnconso ingredient
			ON ingredient_rxnrel.rxcui1 = ingredient.rxcui
			AND ingredient.tty = 'MIN'
			AND ingredient.sab = 'RXNORM'		
		WHERE brand.tty = 'BN'
			AND brand.sab = 'RXNORM'
	) sq
)
SELECT DISTINCT
	brand.rxcui AS brand_rxcui
	, brand.str AS brand_name
	, brand.tty AS brand_tty
	, cte.ingredient_rxcui AS ingredient_rxcui
from datasource.rxnorm_rxnconso product
INNER join datasource.rxnorm_rxnrel rxnrel on rxnrel.rxcui2 = product.rxcui and rxnrel.rela = 'has_ingredient'
INNER join datasource.rxnorm_rxnconso brand
	on rxnrel.rxcui1 = brand.rxcui
	and brand.tty = 'BN'
	and brand.sab = 'RXNORM'
LEFT JOIN cte ON cte.brand_rxcui = brand.rxcui AND cte.rn < 2
where product.tty = 'SBD'
	and product.sab = 'RXNORM';
