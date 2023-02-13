/* datasource.fda_edlr */
DROP TABLE IF EXISTS datasource.fda_edlr;

CREATE TABLE datasource.fda_edlr (
fei_number	                TEXT,
duns_number	                TEXT,
firm_name	                TEXT,
address_text                TEXT,
expiration_date             TEXT,
operations                  TEXT,
establishment_contact_name  TEXT,
establishment_contact_email TEXT,
agent_details               TEXT,
registrant_name             TEXT,
registrant_duns             TEXT,
registrant_contact_name     TEXT,
registrant_contact_email    TEXT,
exclusion_flag              TEXT,
blank                       TEXT
);

COPY datasource.fda_edlr
FROM '{{ ti.xcom_pull(key='file_path',task_ids='get_fda_edlr') }}/drls_reg.txt' DELIMITER E'\t' CSV HEADER ENCODING 'windows-1251';

UPDATE datasource.fda_edlr SET 
                            fei_number = ltrim(rtrim(fei_number)),
                            duns_number = ltrim(rtrim(duns_number)),
                            firm_name = ltrim(rtrim(firm_name)),
                            address_text = ltrim(rtrim(address_text)),
                            expiration_date = ltrim(rtrim(expiration_date)),
                            operations = ltrim(rtrim(operations)),
                            establishment_contact_name = ltrim(rtrim(establishment_contact_name)),
                            establishment_contact_email = ltrim(rtrim(establishment_contact_email)),
                            agent_details = ltrim(rtrim(agent_details)),
                            registrant_name = ltrim(rtrim(registrant_name)),
                            registrant_duns = ltrim(rtrim(registrant_duns)),
                            registrant_contact_name = ltrim(rtrim(registrant_contact_name)),
                            registrant_contact_email = ltrim(rtrim(registrant_contact_email)),
                            exclusion_flag = ltrim(rtrim(exclusion_flag));