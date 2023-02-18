/* datasource.rxnorm_rxnconso */
DROP TABLE IF EXISTS datasource.rxnorm_rxnconso CASCADE;

CREATE TABLE datasource.rxnorm_rxnconso (
rxcui		VARCHAR(8) NOT NULL,
lat			VARCHAR (3) DEFAULT 'ENG' NOT NULL,
ts			VARCHAR (1),
lui			VARCHAR(8),
stt			VARCHAR (3),
sui			VARCHAR (8),
ispref		VARCHAR (1),
rxaui		VARCHAR(8) NOT NULL,
saui		VARCHAR (50),
scui		VARCHAR (50),
sdui		VARCHAR (50),
sab			VARCHAR (20) NOT NULL,
tty			VARCHAR (20) NOT NULL,
code		VARCHAR (50) NOT NULL,
str			TEXT NOT NULL,
srl			VARCHAR (10),
suppress	VARCHAR (1),
cvf			VARCHAR(50),
blank       TEXT
);

COPY datasource.rxnorm_rxnconso FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNCONSO.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default
