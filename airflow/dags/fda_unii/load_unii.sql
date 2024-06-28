/* sagerx_lake.fda_unii */
DROP TABLE IF EXISTS sagerx_lake.fda_unii CASCADE;

CREATE TABLE sagerx_lake.fda_unii (
    unii                  TEXT NOT NULL,
    display_name          TEXT,
    rn                    TEXT,
    ec                    TEXT,
    ncit                  TEXT,
    rxcui                 TEXT,
    pubchem               TEXT,
    epa_comptox           TEXT,
    catalogue_of_life     TEXT,
    itis                  TEXT,
    ncbi                  TEXT,
    plants                TEXT,
    grin                  TEXT,
    mpns                  TEXT,
    inn_id                TEXT,
    usan_id               TEXT,
    mf                    TEXT,
    inchikey              TEXT,
    smiles                TEXT,
    ingredient_type       TEXT,
    substance_type        TEXT,
    uuid                  TEXT,
    dailymed              TEXT
);

COPY sagerx_lake.fda_unii
FROM '{data_path}/{file_name}' DELIMITER E'\t' CSV HEADER ENCODING 'WIN1252';;
