/* sagerx_lake.quicksortrx_qumi */
DROP TABLE IF EXISTS sagerx_lake.quicksortrx_qumi CASCADE;

CREATE TABLE sagerx_lake.quicksortrx_qumi (
    ndc                 TEXT,
    rxcui               TEXT,
    qumi_code           TEXT,
    package_count       TEXT,
    supplier            TEXT,
    description         TEXT,
    dosage_form         TEXT,
    dosage_route        TEXT,
    strength            TEXT,
    measure             TEXT,
    anda                TEXT,
    generic_description TEXT,
    dea                 TEXT
);

COPY sagerx_lake.quicksortrx_qumi
FROM '{data_path}' DELIMITER ',' CSV HEADER ENCODING 'UTF8';

CREATE INDEX IF NOT EXISTS x_ndc
ON sagerx_lake.quicksortrx_qumi(ndc);

CREATE INDEX IF NOT EXISTS x_rxcui
ON sagerx_lake.quicksortrx_qumi(rxcui);

CREATE INDEX IF NOT EXISTS x_qumi_code
ON sagerx_lake.quicksortrx_qumi(qumi_code);

