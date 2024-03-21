/* sagerx_lake.dailymed_rxnorm */
DROP TABLE IF EXISTS sagerx_lake.dailymed_rxnorm CASCADE;

CREATE TABLE sagerx_lake.dailymed_rxnorm (
setid           TEXT,
spl_version     TEXT,
rxcui           TEXT,
rxstr           TEXT,
rxtty           TEXT
);

COPY sagerx_lake.dailymed_rxnorm
FROM '{data_path}/rxnorm_mappings.txt' DELIMITER '|' QUOTE E'\b' CSV HEADER;
