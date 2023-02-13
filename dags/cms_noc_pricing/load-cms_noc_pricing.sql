/* datasource.cms_noc_pricing */
DROP TABLE IF EXISTS datasource.cms_noc_pricing CASCADE;

CREATE TABLE datasource.cms_noc_pricing (
generic_name      TEXT,
dosage            TEXT,
payment_limit     TEXT,
notes             TEXT,
blank             TEXT
);

COPY datasource.cms_noc_pricing
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_noc_pricing') }}/ -name "*NOC Pricing File*.csv")
				tail -n +13 "$ds_path"'
CSV HEADER ENCODING 'ISO-8859-1';