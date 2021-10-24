/* datasource.cms_asp_pricing */
DROP TABLE IF EXISTS datasource.cms_asp_pricing;

CREATE TABLE datasource.cms_asp_pricing (
hcpcs               TEXT,
short_description   TEXT,
dosage              TEXT,
payment_limit       TEXT,
vaccine_awp         TEXT,
vaccine_limit       TEXT,
blood_awp           TEXT,
blood_limit         TEXT,
clotting_factor     TEXT,
notes               TEXT
);

COPY datasource.cms_asp_pricing
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_asp_pricing') }}/ -name "*ASP Pricing File*.csv")
				tail -n +10 "$ds_path"'
CSV HEADER ENCODING 'ISO-8859-1';