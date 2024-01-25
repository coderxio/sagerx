/* sagerx_lake.orange_book_products */
DROP TABLE IF EXISTS sagerx_lake.orange_book_products;

CREATE TABLE sagerx_lake.orange_book_products (
ingredient             TEXT,
df_route               TEXT,
trade_name             TEXT,
applicant              TEXT,
strength               TEXT,
appl_type              TEXT,
appl_no                TEXT,
product_no             TEXT,
te_code                TEXT,
approval_date          TEXT,
rld                    TEXT,
rs                     TEXT,
type                   TEXT,
applicant_full_name    TEXT
);

COPY sagerx_lake.orange_book_products
FROM '{data_path}/products.txt' DELIMITER '~' CSV HEADER;