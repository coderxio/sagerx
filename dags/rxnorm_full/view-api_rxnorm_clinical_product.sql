/*  public.api_rxnorm_clinical_product(scd/sbd/gpck/bpck) */
CREATE OR REPLACE VIEW public.api_rxnorm_clinical_product
AS 
SELECT *
FROM staging.rxnorm_clinical_product;
