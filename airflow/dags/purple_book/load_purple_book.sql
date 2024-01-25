/* sagerx_lake.purple_book */
DROP TABLE IF EXISTS sagerx_lake.purple_book;

CREATE TABLE sagerx_lake.purple_book (
nru											TEXT,
applicant									TEXT,
bla_number									TEXT,
proprietary_name							TEXT,
proper_name									TEXT,
bla_type									TEXT,
strength									TEXT,
dosage_form									TEXT,
route_of_administration						TEXT,
product_presentation						TEXT,
status										TEXT,
licensure									TEXT,
approval_date								TEXT,
ref_product_proper_name						TEXT,
ref_Product_proprietary_name				TEXT,
supplement_number							TEXT,
submission_type								TEXT,
license_number								TEXT,
product_number								TEXT,
center										TEXT,
date_of_first_licensure						TEXT,
exclusivity_expiration_date					TEXT,
first_interchangeable_exclusivity_exp_date	TEXT,
ref_product_exclusivity_exp_date			TEXT,
orphan_exclusivity_exp_date					TEXT
);

COPY sagerx_lake.purple_book
FROM '{data_path}' DELIMITER ',' CSV HEADER QUOTE '"';
