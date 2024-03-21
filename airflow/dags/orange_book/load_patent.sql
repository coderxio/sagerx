/* sagerx_lake.orange_book_patent */
DROP TABLE IF EXISTS sagerx_lake.orange_book_patent;

CREATE TABLE sagerx_lake.orange_book_patent (
appl_type                   TEXT,
appl_no                     TEXT,
product_no                  TEXT,
patent_no                   TEXT,
patent_expire_date_text     TEXT,
drug_substance_flag         TEXT,
drug_product_flag           TEXT,
patent_use_code             TEXT,
delist_flag                 TEXT,
submission_date             TEXT
);

COPY sagerx_lake.orange_book_patent
FROM '{data_path}/patent.txt' DELIMITER '~' CSV HEADER;