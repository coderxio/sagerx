/* sagerx_lake.rxnorm_rxnsty */
DROP TABLE IF EXISTS sagerx_lake.rxnorm_rxnsty CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxnsty (
   rxcui          varchar(8) NOT NULL,
   tui            varchar (4),
   stn            varchar (100),
   sty            varchar (50),
   atui           varchar (11),
   cvf            varchar (50),
   blank          TEXT
);

COPY sagerx_lake.rxnorm_rxnsty
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNSTY.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default