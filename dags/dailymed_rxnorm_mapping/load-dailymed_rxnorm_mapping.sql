/* datasource.dailymed_rxnorm_mapping */
DROP TABLE IF EXISTS datasource.dailymed_rxnorm_mapping;

CREATE TABLE datasource.dailymed_rxnorm_mapping (
setid           TEXT,
spl_version     TEXT,
rxcui           TEXT,
rxstr           TEXT,
rxtty           TEXT
);

COPY datasource.dailymed_rxnorm_mapping
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_dailymed_rxnorm_mapping') }}/rxnorm_mappings.txt' DELIMITER '|' CSV HEADER;
