/*  public.rxnorm_ingredient(in/min) */
CREATE OR REPLACE VIEW public.rxnorm_ingredient
AS 
SELECT
	ingredient.rxcui ingredient_rxcui
	, ingredient.str ingredient_name
	, ingredient.tty ingredient_tty
	, CASE WHEN ingredient.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN ingredient.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso ingredient
WHERE ingredient.tty IN('IN', 'MIN')
	AND ingredient.sab = 'RXNORM'
LIMIT 100;
