/* sagerx_lake.rxnorm_rxncuichanges */

DROP TABLE IF EXISTS  sagerx_lake.rxnorm_rxncuichanges CASCADE;

CREATE TABLE sagerx_lake.rxnorm_rxncuichanges (
      rxaui         varchar(8),
      code          varchar(50),
      sab           varchar(20),
      tty           varchar(20),
      str           varchar(3000),
      old_rxcui     varchar(8) not null,
      new_rxcui     varchar(8) NOT NULL,
      blank         TEXT
);

COPY sagerx_lake.rxnorm_rxncuichanges
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNCUICHANGES.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default
