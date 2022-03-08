/* datasource.dailymed_zip_file_metadata_mapping */
DROP TABLE IF EXISTS datasource.dailymed_zip_file_metadata_mapping;

CREATE TABLE datasource.dailymed_zip_file_metadata_mapping (
setid           TEXT,
zip_file_name   TEXT,
upload_date     TEXT,
spl_version     TEXT,
title           TEXT
);

COPY datasource.dailymed_zip_file_metadata_mapping
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_dailymed_zip_file_metadata_mapping') }}/dm_spl_zip_files_meta_data.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
