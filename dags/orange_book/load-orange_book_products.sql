/* datasource.orange_book_products */
DROP TABLE IF EXISTS datasource.orange_book_products;

CREATE TABLE datasource.orange_book_products (
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

COPY datasource.orange_book_products
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_orange_book') }}/products.txt' DELIMITER '~' CSV HEADER;