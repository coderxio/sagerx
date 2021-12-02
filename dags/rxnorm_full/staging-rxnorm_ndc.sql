/* staging.rxnorm_ndc */
DROP TABLE IF EXISTS staging.rxnorm_ndc CASCADE;

CREATE TABLE staging.rxnorm_ndc (
    ndc        			 varchar(12) PRIMARY KEY,
    clinical_rxcui       varchar(8),
    clinical_tty         varchar(20),
    clinical_str   		 TEXT,
    brand_rxcui          varchar(8),
	brand_tty			 varchar(20),
    brand_str        	 TEXT
);

INSERT INTO staging.rxnorm_ndc
SELECT rxnsat.atv as ndc
	,CASE WHEN rxnconso.tty IN ('BPCK','SBD') THEN relc.RXCUI 
		ELSE rxnsat.rxcui END AS clinical_rxcui
	,CASE WHEN rxnconso.tty IN ('BPCK','SBD') THEN relc.tty
		ELSE rxnconso.tty END AS clinical_tty
	,CASE WHEN rxnconso.tty IN ('BPCK','SBD') THEN relc.str
		ELSE rxnconso.str END AS clinical_str
		
	,CASE WHEN rxnconso.tty IN ('BPCK','SBD') THEN rxnsat.rxcui ELSE NULL END AS brand_rxcui
	,CASE WHEN rxnconso.tty IN ('BPCK','SBD') THEN rxnconso.tty ELSE NULL END AS brand_tty
	,CASE WHEN rxnconso.tty IN ('BPCK','SBD') THEN rxnconso.str ELSE NULL END AS brand_str

FROM datasource.rxnorm_rxnsat rxnsat
	INNER JOIN datasource.rxnorm_rxnconso rxnconso ON rxnsat.rxaui = rxnconso.rxaui
	LEFT JOIN datasource.rxnorm_rxnrel sbdrel ON rxnsat.RXCUI = sbdrel.RXCUI2 AND RELA = 'tradename_of' and rxnconso.tty IN ('BPCK','SBD')
	LEFT JOIN datasource.rxnorm_rxnconso relc
		ON sbdrel.RXCUI1 = relc.RXCUI
		AND relc.tty IN ('SCD','GPCK')
		AND relc.sab = 'RXNORM'
WHERE rxnsat.atn = 'NDC'
	AND rxnconso.tty in ('SCD','SBD','GPCK','BPCK')
	AND rxnconso.sab = 'RXNORM';
