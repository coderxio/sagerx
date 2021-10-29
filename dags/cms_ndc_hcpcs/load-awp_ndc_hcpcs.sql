/* datasource.awp_ndc_hcpcs */
DROP TABLE IF EXISTS datasource.awp_ndc_hcpcs;

CREATE TABLE datasource.awp_ndc_hcpcs (

hcpcs               TEXT,
short_description   TEXT,
labeler_name        TEXT,
ndc                 TEXT,
drug_name           TEXT,
hcpcs_dosage        TEXT,
pkg_size            TEXT,
pkg_qty             TEXT,
billing_units       TEXT,
bill_units_pkg      TEXT
);

COPY datasource.awp_ndc_hcpcs
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_ndc_hcpcs') }}/ -name "*AWP NDC-HCPCS*.csv")
				tail -n +9 "$ds_path"'
CSV HEADER ENCODING 'ISO-8859-1';