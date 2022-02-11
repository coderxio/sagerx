/* datasource.cms_addendum_a */
DROP TABLE IF EXISTS datasource.cms_addendum_a CASCADE;
CREATE TABLE datasource.cms_addendum_a (
apc                            TEXT,
group_title                    TEXT,
si                             TEXT,
relative_weight                TEXT,
payment_rate                   TEXT,
nation_unadjusted_copay        TEXT,
min_unadjusted_copay           TEXT,
note_column                    TEXT,
pass_through_expiration_year   TEXT,
change_flag                    TEXT
);
COPY datasource.cms_addendum_a
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_addendum_a') }}/ -name "*Addendum A.txt")
                tail -n +2 "$ds_path"'
CSV HEADER DELIMITER E'\t' ENCODING 'ISO-8859-1';