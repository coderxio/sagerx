/* datasource.dailymed_pharm_class_mapping */
DROP TABLE IF EXISTS datasource.dailymed_pharm_class_mapping;

CREATE TABLE datasource.dailymed_pharm_class_mapping (
spl_setid           TEXT,
spl_version         TEXT,
pharma_setid        TEXT,
pharma_version      TEXT
);

COPY datasource.dailymed_pharm_class_mapping
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_dailymed_pharm_class_mapping') }}/pharmacologic_class_mappings.txt' DELIMITER '|' CSV HEADER;
