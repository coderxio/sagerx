/*  public.api_rxnorm_ingredient(in/min) */
CREATE OR REPLACE VIEW public.api_rxnorm_ingredient
AS 
SELECT *
FROM staging.rxnorm_ingredient;
