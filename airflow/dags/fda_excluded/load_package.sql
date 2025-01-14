/* sagerx_lake.fda_excluded_package */
DROP TABLE IF EXISTS sagerx_lake.fda_excluded_package CASCADE;

CREATE TABLE sagerx_lake.fda_excluded_package (
productid           TEXT NOT NULL,
productndc          TEXT NOT NULL,
ndcpackagecode      TEXT,
packagedescription  TEXT,
startmarketingdate  TEXT,
endmarketingdate    TEXT,
ndc_exclude_flag    TEXT,
sample_package      TEXT
);

COPY sagerx_lake.fda_excluded_package
FROM '{data_path}/Packages_excluded.txt' DELIMITER E'\t' CSV HEADER ENCODING 'WIN1252';;

CREATE INDEX IF NOT EXISTS x_productid
ON sagerx_lake.fda_excluded_package(productid);