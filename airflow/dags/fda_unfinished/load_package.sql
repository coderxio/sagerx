/* sagerx_lake.fda_unfinished_package */
DROP TABLE IF EXISTS sagerx_lake.fda_unfinished_package CASCADE;

CREATE TABLE sagerx_lake.fda_unfinished_package (
productid           TEXT NOT NULL,
productndc          TEXT NOT NULL,             
ndcpackagecode      TEXT,
packagedescription  TEXT,
startmarketingdate  TEXT,
endmarketingdate    TEXT
);

COPY sagerx_lake.fda_unfinished_package
FROM '{data_path}/unfinished_package.txt' DELIMITER E'\t' CSV HEADER ENCODING 'WIN1252';

CREATE INDEX IF NOT EXISTS x_productid
ON sagerx_lake.fda_unfinished_package(productid);