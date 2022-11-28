/*  public.api_dailymed_rxnorm */
TRUNCATE public.api_dailymed_rxnorm;
INSERT INTO public.api_dailymed_rxnorm
SELECT
	CONCAT(setid, rxcui) AS id
	, setid
	, rxcui
	, rxstr
	, rxtty
FROM datasource.dailymed_rxnorm
WHERE rxtty IN ('SCD', 'SBD', 'GPCK', 'BPCK');
