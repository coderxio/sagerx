/* datasource.fda_ndc_product */
DROP TABLE IF EXISTS datasource.fda_ndc_product CASCADE;

CREATE TABLE datasource.fda_ndc_product (
productid                           TEXT, 
productndc                          TEXT, 
producttypename                     TEXT, 
proprietaryname                     TEXT, 
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
ndc_exclude_flag                    TEXT,
listing_record_certified_through    TEXT,
PRIMARY KEY (productid)
);

COPY datasource.fda_ndc_product
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_ndc') }}/product.txt' DELIMITER E'\t' CSV HEADER ENCODING 'WIN1252';;