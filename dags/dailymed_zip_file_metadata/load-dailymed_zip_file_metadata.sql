/* datasource.dailymed_zip_file_metadata */
DROP TABLE IF EXISTS datasource.dailymed_zip_file_metadata;

CREATE TABLE datasource.dailymed_zip_file_metadata (
setid           TEXT,
zip_file_name   TEXT,
upload_date     TEXT,
spl_version     TEXT,
title           TEXT
);

COPY datasource.dailymed_zip_file_metadata
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_dailymed_zip_file_metadata') }}/dm_spl_zip_files_meta_data.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
