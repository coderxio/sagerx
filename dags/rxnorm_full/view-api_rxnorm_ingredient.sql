/*  public.api_rxnorm_ingredient(in/min) */
TRUNCATE TABLE public.api_rxnorm_ingredient;
INSERT INTO public.api_rxnorm_ingredient
SELECT *
FROM staging.rxnorm_ingredient;
