/*  public.api_rxnorm_product(scd/sbd/gpck/bpck) */
CREATE OR REPLACE VIEW public.api_rxnorm_product
AS 
SELECT rxcui, name, tty, active, prescribable FROM staging.rxnorm_clinical_product
UNION
SELECT rxcui, name, tty, active, prescribable FROM staging.rxnorm_brand_product
WHERE active AND prescribable;
