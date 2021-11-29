/* datasource.rxnorm_rxndoc */

DROP TABLE IF EXISTS datasource.rxnorm_rxndoc;

CREATE TABLE datasource.rxnorm_rxndoc (
    dockey      varchar(50) NOT NULL,
    value       varchar(1000),
    type        varchar(50) NOT NULL,
    expl        varchar(1000),
    blank       TEXT
);

COPY datasource.rxnorm_rxndoc
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_rxnorm_full') }}/rrf/RXNDOC.RRF' with (delimiter '|', null '');
