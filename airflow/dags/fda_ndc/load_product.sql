/* sagerx_lake.fda_ndc_product */
DROP TABLE IF EXISTS sagerx_lake.fda_ndc_product CASCADE;

CREATE TABLE sagerx_lake.fda_ndc_product (
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

COPY sagerx_lake.fda_ndc_product
FROM '{data_path}/product.txt' DELIMITER E'\t' CSV HEADER ENCODING 'WIN1252';;

CREATE INDEX IF NOT EXISTS x_productid
ON sagerx_lake.fda_ndc_product(productid);