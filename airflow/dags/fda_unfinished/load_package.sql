/* datasource.fda_unfinished_package */
DROP TABLE IF EXISTS datasource.fda_unfinished_package CASCADE;

CREATE TABLE datasource.fda_unfinished_package (
productid           TEXT NOT NULL,
productndc          TEXT NOT NULL,             
ndcpackagecode      TEXT,
packagedescription  TEXT,
startmarketingdate  TEXT,
endmarketingdate    TEXT
);

COPY datasource.fda_unfinished_package
FROM '{data_path}/unfinished_package.txt' DELIMITER E'\t' CSV HEADER ENCODING 'WIN1252';