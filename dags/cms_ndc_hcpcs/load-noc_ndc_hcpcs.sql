/* datasource.noc_ndc_hcpcs */
DROP TABLE IF EXISTS datasource.noc_ndc_hcpcs;

CREATE TABLE datasource.noc_ndc_hcpcs (

generic_name        TEXT,
labeler_name        TEXT,
ndc                 TEXT,
drug_name           TEXT,
doseage             TEXT,
pkg_size            TEXT,
pkg_qty             TEXT,
billing_units       TEXT,
bill_units_pkg      TEXT
);

COPY datasource.noc_ndc_hcpcs
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_ndc_hcpcs') }}/ -name "*NOC NDC-HCPCS*.csv")
				tail -n +10 "$ds_path"'
CSV HEADER ENCODING 'ISO-8859-1';