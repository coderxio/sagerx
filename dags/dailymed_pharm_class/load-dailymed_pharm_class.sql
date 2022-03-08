/* datasource.dailymed_pharm_class */
DROP TABLE IF EXISTS datasource.dailymed_pharm_class;

CREATE TABLE datasource.dailymed_pharm_class (
spl_setid           TEXT,
spl_version         TEXT,
pharma_setid        TEXT,
pharma_version      TEXT
);

COPY datasource.dailymed_pharm_class
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_dailymed_pharm_class') }}/pharmacologic_class_mappings.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
