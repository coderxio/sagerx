/* datasource.rxnorm_rxnsty */
DROP TABLE IF EXISTS datasource.rxnorm_rxnsty;

CREATE TABLE datasource.rxnorm_rxnsty (
   rxcui          varchar(8) NOT NULL,
   tui            varchar (4),
   stn            varchar (100),
   sty            varchar (50),
   atui           varchar (11),
   cvf            varchar (50),
   blank          TEXT
);

COPY datasource.rxnorm_rxnsty
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxnorm_full') }}/rrf/RXNSTY.RRF' with (delimiter '|', null '');