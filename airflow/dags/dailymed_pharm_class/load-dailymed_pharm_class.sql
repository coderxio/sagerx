/* sagerx_lake.dailymed_pharm_class */
DROP TABLE IF EXISTS sagerx_lake.dailymed_pharm_class;

CREATE TABLE sagerx_lake.dailymed_pharm_class (
spl_setid           TEXT,
spl_version         TEXT,
pharma_setid        TEXT,
pharma_version      TEXT
);

COPY sagerx_lake.dailymed_pharm_class
FROM '{data_path}/pharmacologic_class_mappings.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
