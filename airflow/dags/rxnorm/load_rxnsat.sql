/* sagerx_lake.rxnorm_rxnsat */
DROP TABLE IF EXISTS sagerx_lake.rxnorm_rxnsat CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxnsat (
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
blank		     TEXT
);

COPY sagerx_lake.rxnorm_rxnsat
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNSAT.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default

CREATE INDEX IF NOT EXISTS rxnsat_rxcui
ON sagerx_lake.rxnorm_rxnsat(rxcui);

CREATE INDEX IF NOT EXISTS rxnsat_atv
ON sagerx_lake.rxnorm_rxnsat(atv);

CREATE INDEX IF NOT EXISTS rxnsat_atn
ON sagerx_lake.rxnorm_rxnsat(atn);
