/* datasource.fda_ndc_package */
DROP TABLE IF EXISTS datasource.fda_ndc_package;

CREATE TABLE datasource.fda_ndc_package (
productid           TEXT NOT NULL,
productndc          TEXT NOT NULL,
ndcpackagecode      TEXT,
packagedescription  TEXT,
startmarketingdate  TEXT,
endmarketingdate    TEXT,
ndc_exclude_flag    TEXT,
sample_package      TEXT
);

COPY datasource.fda_ndc_package
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_ndc') }}/package.txt' WITH (DELIMITER E'\t', NULL '', ENCODING 'WIN1252');