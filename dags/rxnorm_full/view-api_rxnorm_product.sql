/*  public.api_rxnorm_product(scd/sbd/gpck/bpck) */
TRUNCATE TABLE public.api_rxnorm_product;
INSERT INTO public.api_rxnorm_product
SELECT rxcui, name, tty, active, prescribable FROM staging.rxnorm_clinical_product
UNION
SELECT rxcui, name, tty, active, prescribable FROM staging.rxnorm_brand_product
WHERE active AND prescribable;