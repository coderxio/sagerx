/* datasource.nppes_npi_othername */

DROP TABLE IF EXISTS datasource.nppes_npi_othername;

CREATE TABLE datasource.nppes_npi_othername (
npi                     TEXT,
organization_name       TEXT,
type_code               TEXT
);

COPY datasource.nppes_npi_othername   
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_nppes_npi') }}/ -name       "othername_pfile*[0-9].csv")
                tail -n +2 "$ds_path"'
CSV QUOTE '"';