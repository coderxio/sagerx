/* datasource.cms_addendum_a */
DROP TABLE IF EXISTS datasource.cms_addendum_a;
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
effective_date                 TEXT,
activation_date                TEXT,
termination_date               TEXT,
change_flag                    TEXT
);
COPY datasource.cms_addendum_a
FROM PROGRAM 'ds_path=$(find {{ ti.xcom_pull(key='file_path',task_ids='get_cms_addendum_a') }}/ -name "*Addendum_A*.csv")
                tail -n +3 "$ds_path"'
CSV HEADER ENCODING 'ISO-8859-1';