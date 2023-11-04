/* datasource.purple_book_extract */
DROP TABLE IF EXISTS datasource.purple_book_extract;

CREATE TABLE datasource.purple_book_extract (
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
orphan_exclusivity_exp_date					TEXT,
blank										TEXT
);

COPY datasource.purple_book_extract
FROM PROGRAM 'ds_path={data_path}
			lineNum="$(grep -n "Purple Book Database Extract" $ds_path | head -n 1 | cut -d: -f1)"
			lineNum=$((lineNum + 1))
			tail -n +$lineNum "$ds_path"'
CSV HEADER QUOTE '"';
