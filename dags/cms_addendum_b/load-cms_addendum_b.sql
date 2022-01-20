/* datasource.cms_addendum_b */

DROP TABLE IF EXISTS datasource.cms_addendum_b;

CREATE TABLE datasource.cms_addendum_b (
hcpcs                           TEXT,
short_descriptor                TEXT,
si                              TEXT,
apc                             TEXT,
relative_weight                 TEXT,
payment_rate                    TEXT,
national_unadjusted_copay       TEXT,
min_unadjusted_copay            TEXT,
note_column                     TEXT,
pass_through_expiration_year	TEXT,
change_flag                     TEXT
);

COPY datasource.cms_addendum_b   
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_addendum_b') }}/ -name       "*Addendum B.txt")
                tail -n +4 "$ds_path"'
CSV HEADER DELIMITER E'\t' ENCODING 'ISO-8859-1';