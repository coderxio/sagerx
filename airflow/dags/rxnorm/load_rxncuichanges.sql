/* datasource.rxnorm_rxncuichanges */

DROP TABLE IF EXISTS  datasource.rxnorm_rxncuichanges CASCADE;

CREATE TABLE datasource.rxnorm_rxncuichanges (
      rxaui         varchar(8),
      code          varchar(50),
      sab           varchar(20),
      tty           varchar(20),
      str           varchar(3000),
      old_rxcui     varchar(8) not null,
      new_rxcui     varchar(8) NOT NULL,
      blank         TEXT
);

COPY datasource.rxnorm_rxncuichanges
FROM '{{ ti.xcom_pull(task_ids='extract') }}/rrf/RXNCUICHANGES.RRF' CSV DELIMITER '|' ENCODING 'UTF8' ESCAPE E'\b' QUOTE E'\b';
--ESCAPE and QOUTE characters are dummy to remove default
