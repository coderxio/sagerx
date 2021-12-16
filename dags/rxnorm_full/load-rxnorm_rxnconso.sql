/* datasource.rxnorm_rxnconso */
DROP TABLE IF EXISTS datasource.rxnorm_rxnconso CASCADE;

CREATE TABLE datasource.rxnorm_rxnconso (
rxcui		varchar(8) NOT NULL,
lat			varchar (3) DEFAULT 'ENG' NOT NULL,
ts			varchar (1),
lui			varchar(8),
stt			varchar (3),
sui			varchar (8),
ispref		varchar (1),
rxaui		varchar(8) NOT NULL,
saui		varchar (50),
scui		varchar (50),
sdui		varchar (50),
sab			varchar (20) NOT NULL,
tty			varchar (20) NOT NULL,
code		varchar (50) NOT NULL,
str			TEXT NOT NULL,
srl			varchar (10),
supress		varchar (1),
cvf			varchar(50),
blank       TEXT
);

COPY datasource.rxnorm_rxnconso FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxnorm_full') }}/rrf/RXNCONSO.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default
