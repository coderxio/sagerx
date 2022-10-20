/*  public.api_rxterms_strength */
TRUNCATE TABLE public.api_rxterms_strength;
INSERT INTO public.api_rxterms_strength
SELECT *
FROM staging.rxterms_strength;
