/* sagerx_lake.ncpdp */
DROP TABLE IF EXISTS sagerx_lake.ncpdp CASCADE;

CREATE TABLE sagerx_lake.ncpdp (
    ncit_subset_code           TEXT,
    ncpdp_subset_preferred_term TEXT,
    ncit_code                  TEXT,
    ncpdp_preferred_term       TEXT,
    ncpdp_synonym              TEXT,
    ncit_preferred_term        TEXT,
    nci_definition             TEXT
);

COPY sagerx_lake.ncpdp
FROM '{data_path}' DELIMITER E'\t' CSV HEADER ENCODING 'UTF8';

CREATE INDEX IF NOT EXISTS x_ncit_subset_code
ON sagerx_lake.ncpdp(ncit_subset_code);

CREATE INDEX IF NOT EXISTS x_ncit_code
ON sagerx_lake.ncpdp(ncit_code);
