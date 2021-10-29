/* datasource.fda_unfinished_package */
DROP TABLE IF EXISTS datasource.fda_unfinished_package;

CREATE TABLE datasource.fda_unfinished_package (
productid           TEXT NOT NULL,
productndc          TEXT NOT NULL,             
ndcpackagecode      TEXT,
packagedescription  TEXT,
startmarketingdate  TEXT,
endmarketingdate    TEXT
);

COPY datasource.fda_unfinished_package
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_unfinished') }}/unfinished_package.txt' (DELIMITER E'\t', NULL '', ENCODING 'WIN1252');