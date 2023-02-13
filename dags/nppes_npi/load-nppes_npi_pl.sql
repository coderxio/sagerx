/* datasource.nppes_npi_pl */

DROP TABLE IF EXISTS datasource.nppes_npi_pl;

CREATE TABLE datasource.nppes_npi_pl (
npi                     TEXT,
address_line_1          TEXT,
address_line_2          TEXT,
city_name               TEXT,
state_name              TEXT,
postal_code             TEXT,
country_code            TEXT,
telephone_number        TEXT,
telephone_extension     TEXT,
fax_number              TEXT
);

COPY datasource.nppes_npi_pl   
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_nppes_npi') }}/ -name       "pl_pfile*[0-9].csv")
                tail -n +2 "$ds_path"'
CSV QUOTE '"';