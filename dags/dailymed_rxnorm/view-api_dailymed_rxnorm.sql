/*  public.api_dailymed_rxnorm */
CREATE OR REPLACE VIEW public.api_dailymed_rxnorm
AS 
SELECT
	CONCAT(setid, rxcui) AS id
	, setid
	, rxcui
	, rxstr
	, rxtty
FROM datasource.dailymed_rxnorm;
