/*  public.api_rxterms_name */
TRUNCATE TABLE public.api_rxterms_name;
INSERT INTO public.api_rxterms_name
SELECT *
FROM staging.rxterms_name;
