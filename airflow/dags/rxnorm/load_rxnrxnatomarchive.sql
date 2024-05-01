/* sagerx_lake.rxnorm_rxnatomarchive */
DROP TABLE IF EXISTS sagerx_lake.rxnorm_rxnatomarchive CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxnatomarchive (
   rxaui             varchar(8) not null,
   aui               varchar(10),
   str               varchar(4000) not null,
   archive_timestamp varchar(280) not null,
   created_timestamp varchar(280) not null,
   updated_timestamp varchar(280) not null,
   code              varchar(50),
   is_brand          varchar(1),
   lat               varchar(3),
   last_released     varchar(30),
   saui              varchar(50),
   vsab              varchar(40),
   rxcui             varchar(8),
   sab               varchar(20),
   tty               varchar(20),
   merged_to_rxcui   varchar(8),
   blank             TEXT
);

COPY sagerx_lake.rxnorm_rxnatomarchive
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNATOMARCHIVE.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default

CREATE INDEX IF NOT EXISTS rxnrel_rxaui
ON sagerx_lake.rxnorm_rxnatomarchive(rxaui);

CREATE INDEX IF NOT EXISTS rxnrel_rxcui
ON sagerx_lake.rxnorm_rxnatomarchive(rxcui);

CREATE INDEX IF NOT EXISTS rxnrel_mergedcui
ON sagerx_lake.rxnorm_rxnatomarchive(merged_to_rxcui);