/* datasource.fda_unfinished_product*/
DROP TABLE IF EXISTS datasource.fda_unfinished_product;

CREATE TABLE datasource.fda_unfinished_product (
productid                           TEXT,
productndc                          TEXT,
producttypename                     TEXT,
nonproprietaryname                  TEXT,
dosageformname                      TEXT,
startmarketingdate                  TEXT,
endmarketingdate                    TEXT,
marketingcategoryname               TEXT,
labelername                         TEXT,
substancename                       TEXT,
active_numerator_strength           TEXT,
active_ingred_unit                  TEXT,
deaschedule                         TEXT,
listing_record_certified_through    TEXT
);

COPY datasource.fda_unfinished_product
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_unfinished') }}/unfinished_product.txt' WITH (DELIMITER E'\t', NULL '', ENCODING 'WIN1252');