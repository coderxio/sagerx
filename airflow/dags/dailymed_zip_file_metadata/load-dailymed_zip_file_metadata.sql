/* sagerx_lake.dailymed_zip_file_metadata */
DROP TABLE IF EXISTS sagerx_lake.dailymed_zip_file_metadata;

CREATE TABLE sagerx_lake.dailymed_zip_file_metadata (
setid           TEXT,
zip_file_name   TEXT,
upload_date     TEXT,
spl_version     TEXT,
title           TEXT
);

COPY sagerx_lake.dailymed_zip_file_metadata
FROM '{data_path}/dm_spl_zip_files_meta_data.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
