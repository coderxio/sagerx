/* datasource.nppes_npi_endpoint */

DROP TABLE IF EXISTS datasource.nppes_npi_endpoint;

CREATE TABLE datasource.nppes_npi_endpoint (
npi                                 TEXT,
endpoint_type                       TEXT,
endpoint_type_description           TEXT,
endpoint                            TEXT,
affiliation                         TEXT,
endpoint_description                TEXT,
affiliation_legal_business_name     TEXT,
use_code                            TEXT,
use_description                     TEXT,
other_use_description               TEXT,
content_type                        TEXT,
content_description                 TEXT,
other_content_description           TEXT,
affiliation_address_line_one        TEXT,
affiliation_address_line_two        TEXT,
affiliation_address_city            TEXT,
affiliation_address_state           TEXT,
affiliation_address_country         TEXT,
affiliation_address_postal_code     TEXT
);

COPY datasource.nppes_npi_endpoint   
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_nppes_npi') }}/ -name       "endpoint_pfile*[0-9].csv")
                tail -n +2 "$ds_path"'
CSV QUOTE '"';