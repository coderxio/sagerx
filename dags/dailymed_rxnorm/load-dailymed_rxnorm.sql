/* datasource.dailymed_rxnorm */
DROP TABLE IF EXISTS datasource.dailymed_rxnorm;

CREATE TABLE datasource.dailymed_rxnorm (
setid           TEXT,
spl_version     TEXT,
rxcui           TEXT,
rxstr           TEXT,
rxtty           TEXT
);

COPY datasource.dailymed_rxnorm
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_dailymed_rxnorm') }}/rxnorm_mappings.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
