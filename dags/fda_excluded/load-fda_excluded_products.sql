/* datasource.fda_excluded_package */
DROP TABLE IF EXISTS datasource.fda_excluded_product;

CREATE TABLE datasource.fda_excluded_product (
productid                           TEXT,
productndc                          TEXT,
producttypename                     TEXT,
propietaryname                      TEXT,
proprietarynamesuffix               TEXT,
nonproprietaryname                  TEXT,
dosageformname                      TEXT,
routename                           TEXT,
startmarketingdate                  TEXT,
endmarketingdate                    TEXT,
marketingcategoryname               TEXT,
applicationnumber                   TEXT,
labelername                         TEXT,
substancename                       TEXT,
active_numerator_strength           TEXT,
active_ingred_unit                  TEXT,
pharm_classes                       TEXT,
deaschedule                         TEXT,
ndc_excluded_flag                   TEXT,
listing_record_certified_through    TEXT
);

COPY datasource.fda_excluded_product
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_excluded') }}/Products_excluded.txt' WITH (DELIMITER E'\t', NULL '', ENCODING 'WIN1252');