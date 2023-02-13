/* datasource.fda_wdtp */
DROP TABLE IF EXISTS datasource.fda_wdtp;

CREATE TABLE datasource.fda_wdtp (
type                        TEXT,
facility_name               TEXT,
doing_business_as           TEXT,
facility_street             TEXT,
facility_city               TEXT,
facility_state              TEXT,
license_number              TEXT,
license_state               TEXT,
license_expiration_date     TEXT,
facility_contact_name       TEXT,
facility_contact_phone      TEXT,
facility_contact_email      TEXT,
reporting_year              TEXT
);

COPY datasource.fda_wdtp
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_wdtp') }}' DELIMITER E'\t' CSV HEADER ENCODING 'windows-1251';

UPDATE datasource.fda_wdtp SET 
                            license_number = ltrim(rtrim(license_number))