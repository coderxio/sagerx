/* sagerx_lake.rxnorm_rxndoc */

DROP TABLE IF EXISTS sagerx_lake.rxnorm_rxndoc CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxndoc (
    dockey      varchar(50) NOT NULL,
    value       varchar(1000),
    type        varchar(50) NOT NULL,
    expl        varchar(1000),
    blank       TEXT
);

COPY sagerx_lake.rxnorm_rxndoc
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNDOC.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default
