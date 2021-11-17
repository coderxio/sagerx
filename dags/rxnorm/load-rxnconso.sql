/* datasource.rxnconso */
DROP TABLE IF EXISTS datasource.rxnconso;

CREATE TABLE datasource.rxnconso (
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
str			varchar (3000) NOT NULL,
srl			varchar (10),
supress		varchar (1),
cvf			varchar(50),
blank		varchar(1)
);

COPY datasource.rxnconso
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxnorm') }}/rrf/RXNCONSO.RRF' with (delimiter '|', null '');