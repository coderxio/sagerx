/* datasource.rxnsat */
DROP TABLE IF EXISTS datasource.rxnsat;

CREATE TABLE datasource.rxnsat (
rxcui            varchar(8) ,
lui              varchar(8),
sui              varchar(8),
rxaui            varchar(8),
stype            varchar (50),
code             varchar (50),
atui             varchar(11),
satui            varchar (50),
atn              varchar (1000) NOT NULL,
sab              varchar (20) NOT NULL,
atv              varchar (7000),
suppress         varchar (1),
cvf              varchar (50),
blank		varchar(1)
);

COPY datasource.rxnsat
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxnorm') }}/rrf/RXNSAT.RRF' with (delimiter '|', null '');