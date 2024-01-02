/* datasource.orange_book_exlusivity */
DROP TABLE IF EXISTS datasource.orange_book_exlusivity;

CREATE TABLE datasource.orange_book_exlusivity (
appl_type          TEXT,
appl_no            TEXT,
product_no         TEXT,
exclusivity_code   TEXT,
exclusivity_date   TEXT
);

COPY datasource.orange_book_exlusivity
FROM '{data_path}/exclusivity.txt' DELIMITER '~' CSV HEADER;