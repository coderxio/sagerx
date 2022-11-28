/*  staging.mthspl_substance (SU) */
DROP TABLE IF EXISTS staging.mthspl_substance CASCADE;

CREATE TABLE staging.mthspl_substance (
    rxcui				VARCHAR(8) NOT NULL,
    name 				TEXT,
	tty					VARCHAR(20),
	rxaui				VARCHAR(20),
	unii				VARCHAR(20),
	active				BOOLEAN,
	prescribable		BOOLEAN,
	PRIMARY KEY(rxaui)
);

INSERT INTO staging.mthspl_substance
SELECT
	substance.rxcui rxcui
	, substance.str name
	, substance.tty tty
	, substance.rxaui rxaui
	, substance.code unii
	, CASE WHEN substance.suppress = 'N' THEN TRUE ELSE FALSE END AS active
	, CASE WHEN substance.cvf = '4096' THEN TRUE ELSE FALSE END AS prescribable
FROM datasource.rxnorm_rxnconso substance
WHERE substance.tty = 'SU'
	AND substance.sab = 'MTHSPL';